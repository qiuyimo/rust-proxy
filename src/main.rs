//! 多协议动态代理服务
//!
//! 本服务支持 TCP、UDP、HTTP 和 gRPC 协议的代理转发。
//! 特别地，对于 gRPC 请求，可以通过请求头中的 `x-backend` 字段动态指定后端服务地址。
//!
//! # 功能特性
//! - TCP 透明代理：双向转发 TCP 流量
//! - UDP 代理：转发 UDP 数据包，支持超时控制
//! - HTTP/HTTPS 代理：基于 Hyper 实现的高性能 HTTP 代理
//! - gRPC 动态代理：根据 `x-backend` 请求头动态路由到不同的后端服务
//!
//! # 配置
//! 所有代理配置通过 `config.toml` 文件管理，支持配置多个代理实例。

use anyhow::Result;                    // 灵活的错误处理库
use bytes::Bytes;                      // 高效的字节缓冲区，用于 HTTP 响应体
use http::{Request, Response, StatusCode}; // HTTP 类型定义
use http_body_util::{combinators::BoxBody, BodyExt, Full}; // HTTP body 工具
use hyper::body::Incoming;             // Hyper 的请求体类型（流式读取）
use hyper::server::conn::http1;        // HTTP/1.1 连接处理
use hyper::service::Service;           // 服务 trait，用于处理 HTTP 请求
use hyper_util::rt::TokioIo;           // 将 Tokio 的 I/O 类型适配到 Hyper
use log::{error, info, warn};          // 日志记录
use serde::Deserialize;                // 从 TOML 配置文件反序列化
use std::future::Future;               // 异步 trait 的 Future 类型
use std::net::SocketAddr;              // 网络地址类型
use std::pin::Pin;                     // 固定内存位置的指针，用于异步 Future
use std::sync::Arc;                    // 原子引用计数，用于在线程间共享 UDP socket
use std::time::Duration;               // 时间间隔，用于超时控制
use tokio::net::{TcpListener, TcpStream, UdpSocket}; // Tokio 异步网络类型

// ============================================================================
// 配置结构体
// ============================================================================

/// TCP 代理配置
///
/// 每个 TCP 代理实例监听一个本地端口，并将所有流量转发到指定的后端地址。
#[derive(Debug, Deserialize, Clone)]
struct TcpProxyConfig {
    /// 本地监听地址，格式如 "0.0.0.0:8081"
    listen_addr: String,
    /// 后端目标地址，所有流量将被转发到此地址，格式如 "10.0.0.1:9091"
    backend_addr: String,
}

/// UDP 代理配置
///
/// 每个 UDP 代理实例监听一个本地端口，转发 UDP 数据报到后端。
/// 注意：UDP 是无连接协议，每个数据包独立转发。
#[derive(Debug, Deserialize, Clone)]
struct UdpProxyConfig {
    /// 本地监听地址
    listen_addr: String,
    /// 后端目标地址
    backend_addr: String,
}

/// HTTP 代理配置
///
/// HTTP 代理不支持动态路由，所有请求固定转发到指定的后端。
#[derive(Debug, Deserialize, Clone)]
struct HttpProxyConfig {
    /// 本地监听地址
    listen_addr: String,
    /// 后端目标地址（HTTP 服务的地址）
    backend_addr: String,
}

/// gRPC 动态代理配置
///
/// gRPC 代理的特殊之处在于支持动态路由：
/// - 如果请求头包含 `x-backend`，则路由到该头部指定的地址
/// - 否则，使用 `default_backend` 作为默认后端
#[derive(Debug, Deserialize, Clone)]
struct GrpcProxyConfig {
    /// 本地监听地址
    listen_addr: String,
    /// 默认后端地址（当请求中没有 x-backend 头部时使用）
    default_backend: String,
}

/// 全局配置结构
///
/// 对应 config.toml 文件的顶层结构。
/// 支持配置多组代理实例，每组独立运行。
#[derive(Debug, Deserialize, Clone)]
struct Config {
    /// TCP 代理配置列表（可配置多个 TCP 代理）
    tcp_proxies: Vec<TcpProxyConfig>,
    /// UDP 代理配置列表
    udp_proxies: Vec<UdpProxyConfig>,
    /// HTTP 代理配置列表
    http_proxies: Vec<HttpProxyConfig>,
    /// gRPC 动态代理配置（通常只配置一个）
    grpc_proxy: GrpcProxyConfig,
}

// ============================================================================
// TCP 代理实现
// ============================================================================

/// 运行 TCP 代理服务
///
/// TCP 代理工作原理：
/// 1. 监听本地端口，等待客户端连接
/// 2. 收到客户端连接后，立即连接到后端服务器
/// 3. 在两个连接之间双向复制数据（全双工转发）
/// 4. 任一方向连接关闭时，自动关闭另一个方向
///
/// # 参数
/// - `listen_addr`: 本地监听地址
/// - `backend_addr`: 后端目标地址
///
/// # 错误处理
/// - 连接后端失败时，记录错误日志并关闭客户端连接
/// - 数据传输错误（如连接断开）时，记录警告日志
async fn run_tcp_proxy(listen_addr: String, backend_addr: String) -> Result<()> {
    // 绑定监听端口
    // TcpListener::bind 会创建一个 TCP 监听器
    let listener = TcpListener::bind(&listen_addr).await?;
    info!("TCP proxy listening on {}", listen_addr);

    // 无限循环，持续接受新的客户端连接
    loop {
        // 等待并接受新的客户端连接
        match listener.accept().await {
            Ok((mut client_stream, client_addr)) => {
                // 克隆后端地址，以便在异步任务中使用
                let backend = backend_addr.clone();

                // 为每个客户端连接创建一个新的异步任务
                // tokio::spawn 允许并发处理多个客户端连接
                tokio::spawn(async move {
                    info!("TCP proxy: {} -> {}", client_addr, backend);

                    // 连接到后端服务器
                    match TcpStream::connect(&backend).await {
                        Ok(mut backend_stream) => {
                            // 将客户端流和后端流分别拆分为读/写两部分
                            // split() 返回 (读半部分, 写半部分)
                            // 这样可以同时进行双向数据传输
                            let (mut client_read, mut client_write) = client_stream.split();
                            let (mut backend_read, mut backend_write) = backend_stream.split();

                            // 创建两个异步任务：客户端→后端，后端→客户端
                            // io::copy 会持续读取数据并写入，直到 EOF
                            let client_to_backend =
                                tokio::io::copy(&mut client_read, &mut backend_write);
                            let backend_to_client =
                                tokio::io::copy(&mut backend_read, &mut client_write);

                            // tokio::select! 等待任一方向的传输完成
                            // 这实现了全双工代理：两个方向同时工作
                            // 当任一方向关闭连接时，自动取消另一个方向
                            tokio::select! {
                                r = client_to_backend => {
                                    if let Err(e) = r {
                                        warn!("TCP client->backend error: {}", e);
                                    }
                                }
                                r = backend_to_client => {
                                    if let Err(e) = r {
                                        warn!("TCP backend->client error: {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // 连接后端失败，记录错误
                            error!("Failed to connect to backend {}: {}", backend, e);
                        }
                    }
                });
            }
            Err(e) => {
                // 接受连接失败（可能因为资源耗尽等）
                error!("Failed to accept TCP connection: {}", e);
            }
        }
    }
}

// ============================================================================
// UDP 代理实现
// ============================================================================

/// 运行 UDP 代理服务
///
/// UDP 代理工作原理：
/// 1. 绑定本地 UDP 端口
/// 2. 收到客户端数据包后，记录客户端地址
/// 3. 创建一个临时 UDP socket 发送数据到后端
/// 4. 等待后端响应（最多 30 秒超时）
/// 5. 将后端响应转发回原始客户端
///
/// 注意：UDP 是无连接协议，每次交互使用新的临时 socket
///
/// # 参数
/// - `listen_addr`: 本地监听地址
/// - `backend_addr`: 后端目标地址
async fn run_udp_proxy(listen_addr: String, backend_addr: String) -> Result<()> {
    // 绑定 UDP socket
    let socket = UdpSocket::bind(&listen_addr).await?;
    info!("UDP proxy listening on {}", listen_addr);

    // 使用 Arc 包装 socket，使其可以在多个异步任务间共享
    // 因为 UDP socket 是线程安全的，可以同时读写
    let socket = Arc::new(socket);

    // 预分配缓冲区，避免每次接收时重新分配内存
    // 65535 是 UDP 数据包的最大理论大小
    let mut buf = vec![0u8; 65535];

    loop {
        // 接收来自任意客户端的数据包
        // recv_from 返回 (接收的字节数, 发送方地址)
        match socket.recv_from(&mut buf).await {
            Ok((size, client_addr)) => {
                // 复制实际接收到的数据（buf 可能未完全填满）
                let data = buf[..size].to_vec();
                let backend = backend_addr.clone();
                let socket_clone = socket.clone();

                // 创建异步任务处理这个数据包
                tokio::spawn(async move {
                    // 为本次请求创建临时 UDP socket
                    // 绑定到随机端口（端口 0 表示由操作系统分配）
                    let backend_socket = match UdpSocket::bind("0.0.0.0:0").await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to create UDP socket: {}", e);
                            return;
                        }
                    };

                    // 将客户端数据转发到后端
                    if let Err(e) = backend_socket.send_to(&data, &backend).await {
                        error!("Failed to send to backend: {}", e);
                        return;
                    }

                    // 等待后端响应，设置 30 秒超时
                    let mut recv_buf = vec![0u8; 65535];
                    match tokio::time::timeout(
                        Duration::from_secs(30),   // 超时时间：30秒
                        backend_socket.recv_from(&mut recv_buf), // 等待后端响应
                    )
                        .await
                    {
                        Ok(Ok((recv_size, _))) => {
                            // 成功收到后端响应，转发给原始客户端
                            if let Err(e) = socket_clone
                                .send_to(&recv_buf[..recv_size], client_addr)
                                .await
                            {
                                error!("Failed to send to client: {}", e);
                            }
                        }
                        Ok(Err(e)) => {
                            // 接收后端数据时发生错误
                            error!("Failed to receive from backend: {}", e);
                        }
                        Err(_) => {
                            // 超时：后端在 30 秒内未响应
                            warn!("UDP receive timeout");
                        }
                    }
                });
            }
            Err(e) => {
                error!("UDP receive error: {}", e);
            }
        }
    }
}

// ============================================================================
// HTTP/gRPC 代理服务
// ============================================================================

/// HTTP/gRPC 代理服务
///
/// 实现了 Hyper 的 Service trait，可以处理 HTTP/1.1 和 gRPC 请求。
/// gRPC 本质上基于 HTTP/2，但 Hyper 的 HTTP/1.1 实现也可以处理
/// 某些 gRPC 场景（特别是使用 grpc-web 时）。
///
/// # 动态路由
/// 通过检查请求头中的 `x-backend` 字段实现动态后端选择：
/// - 如果存在该头部，使用其值作为后端地址
/// - 否则使用默认后端地址
#[derive(Clone)]
struct ProxyService {
    /// 默认后端地址（当请求中没有 x-backend 头部时使用）
    default_backend: String,

    /// HTTP 客户端，用于转发请求到后端
    ///
    /// Client 是 Hyper 提供的 HTTP 客户端实现：
    /// - HttpConnector: 负责建立 TCP 连接
    /// - Incoming: 表示从后端接收的响应体（流式）
    client: hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        Incoming,
    >,
}

impl ProxyService {
    /// 创建新的代理服务实例
    ///
    /// # 参数
    /// - `default_backend`: 默认后端地址
    fn new(default_backend: String) -> Self {
        // 创建 HTTP 连接器（负责 DNS 解析和 TCP 连接）
        let connector = hyper_util::client::legacy::connect::HttpConnector::new();

        // 创建 HTTP 客户端
        // builder 模式配置客户端行为
        let client: hyper_util::client::legacy::Client<
            hyper_util::client::legacy::connect::HttpConnector,
            Incoming,
        > = hyper_util::client::legacy::Client::builder(
            hyper_util::rt::TokioExecutor::new()  // 使用 Tokio 作为异步运行时
        )
            .build(connector);

        ProxyService {
            default_backend,
            client,
        }
    }

    /// 从请求中提取后端地址
    ///
    /// 优先级：
    /// 1. 请求头中的 `x-backend` 字段（如果存在）
    /// 2. 默认后端地址
    ///
    /// 这个方法是 gRPC 动态代理的核心逻辑
    fn extract_backend(&self, req: &Request<Incoming>) -> String {
        // 检查是否存在 x-backend 请求头
        if let Some(backend_value) = req.headers().get("x-backend") {
            // 尝试将头部值转换为字符串
            if let Ok(backend_str) = backend_value.to_str() {
                info!("Dynamic gRPC proxy to: {}", backend_str);
                return backend_str.to_string();
            }
        }

        // 没有 x-backend 头部或转换失败时，使用默认后端
        self.default_backend.clone()
    }
}

/// 实现 Hyper 的 Service trait，使 ProxyService 可以处理 HTTP 请求
///
/// Service trait 是 Hyper 的核心抽象，所有 HTTP 处理器都需要实现它。
/// 它定义了如何处理一个 HTTP 请求并返回一个 HTTP 响应。
impl Service<Request<Incoming>> for ProxyService {
    /// 成功响应类型：包含 boxed body 的 HTTP 响应
    /// BoxBody 允许我们在编译时不知道具体 body 类型的情况下返回响应
    type Response = Response<BoxBody<Bytes, hyper::Error>>;

    /// 错误类型：Hyper 的错误类型
    type Error = hyper::Error;

    /// Future 类型：异步返回响应的 Future
    /// Pin<Box<dyn Future>> 是因为我们使用了 async 块，
    /// 需要在堆上分配并固定内存位置
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// 处理 HTTP 请求的核心方法
    ///
    /// 处理流程：
    /// 1. 提取后端地址（检查 x-backend 头部）
    /// 2. 构建转发到后端的请求
    /// 3. 发送请求并等待响应
    /// 4. 将后端响应返回给客户端
    fn call(&self, mut req: Request<Incoming>) -> Self::Future {
        // 第一步：确定要转发到的后端地址
        let backend = self.extract_backend(&req);
        let client = self.client.clone();

        // 第二步：构建后端请求的 URI
        // 保留原始请求的路径和查询参数
        let path_and_query = req
            .uri()
            .path_and_query()          // 获取路径和查询字符串
            .map(|pq| pq.as_str())     // 转换为字符串
            .unwrap_or("/");            // 如果没有路径，使用根路径 "/"

        // 构建完整的后端 URL
        let uri = format!("http://{}{}", backend, path_and_query);

        info!("Forwarding request to: {}", uri);

        // 第三步：移除 x-backend 头部
        // 这个头部是代理专用的，不应该传递给后端服务
        req.headers_mut().remove("x-backend");

        // 使用 Box::pin 将异步块包装为 Pin<Box<dyn Future>>
        // 这是 Service trait 的 Future 类型所要求的
        Box::pin(async move {
            // 第四步：构建转发到后端的请求
            // 使用 Request::builder() 创建新的请求
            let mut backend_req = Request::builder()
                .method(req.method().clone())  // 保持相同的 HTTP 方法
                .uri(&uri);                     // 使用新的 URI

            // 第五步：复制请求头
            // 遍历原始请求的所有头部
            for (key, value) in req.headers().iter() {
                // 跳过 host 头部（我们会重新设置）
                // 跳过 x-backend 头部（代理专用头部）
                if key.as_str() != "host" && key.as_str() != "x-backend" {
                    // 将头部值转换为字符串（某些头部可能包含非 ASCII 字符）
                    if let Ok(v) = value.to_str() {
                        backend_req = backend_req.header(key.as_str(), v);
                    }
                }
            }

            // 设置正确的 Host 头部
            // Host 头部应该指向后端服务器，而不是代理服务器
            if let Some(host) = req.uri().host() {
                backend_req = backend_req.header("host", host);
            }

            // 第六步：将原始请求体附加到后端请求
            // req.into_body() 消费原始请求，返回请求体
            let backend_req = match backend_req.body(req.into_body()) {
                Ok(r) => r,
                Err(e) => {
                    // 构建请求失败（通常是 URI 格式错误）
                    error!("Failed to build backend request: {}", e);

                    // 返回 400 Bad Request 错误
                    let response = Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(
                            // Full::new 创建一个完整的 body（不是流式的）
                            Full::new(Bytes::from(format!("Bad request: {}", e)))
                                .map_err(|never| match never {})  // 类型转换
                                .boxed(),                         // 转换为 BoxBody
                        )
                        .unwrap();
                    return Ok(response);
                }
            };

            // 第七步：将请求转发到后端并等待响应
            match client.request(backend_req).await {
                Ok(response) => {
                    // 成功收到后端响应
                    info!("Backend response status: {}", response.status());

                    // 将响应拆分为头部和体部
                    let (parts, body) = response.into_parts();

                    // 将响应体包装为 BoxBody
                    // map_err 用于类型转换，boxed() 创建 BoxBody
                    let boxed_body = body.map_err(|e| e).boxed();

                    // 重新组合响应（保持相同的状态码和头部，替换 body）
                    Ok(Response::from_parts(parts, boxed_body))
                }
                Err(e) => {
                    // 连接后端失败（网络错误、超时、DNS 解析失败等）
                    error!("Failed to connect to backend: {}", e);

                    // 返回 502 Bad Gateway 错误
                    let response = Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .body(
                            Full::new(Bytes::from(format!("Bad gateway: {}", e)))
                                .map_err(|never| match never {})
                                .boxed(),
                        )
                        .unwrap();
                    Ok(response)
                }
            }
        })
    }
}

/// 运行 HTTP/gRPC 代理服务器
///
/// 使用 Hyper 的 HTTP/1.1 服务器实现。
/// 虽然 gRPC 通常使用 HTTP/2，但 HTTP/1.1 代理也可以处理
/// gRPC-Web 请求和某些 gRPC 客户端。
///
/// # 参数
/// - `listen_addr`: 本地监听地址
/// - `backend_addr`: 默认后端地址
async fn run_http_proxy(listen_addr: String, backend_addr: String) -> Result<()> {
    // 解析监听地址
    let addr: SocketAddr = listen_addr.parse()?;

    // 创建 TCP 监听器
    let listener = TcpListener::bind(addr).await?;
    info!("HTTP/gRPC proxy listening on {}", listen_addr);

    // 创建代理服务实例
    let proxy_service = ProxyService::new(backend_addr);

    // 持续接受新的连接
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                // 每个连接使用独立的 ProxyService 实例（通过 clone）
                // ProxyService 的 clone 是廉价的，因为它内部使用 Arc
                let service = proxy_service.clone();

                // 为每个连接创建异步任务
                tokio::spawn(async move {
                    // 将 Tokio 的 TcpStream 适配为 Hyper 的 IO 类型
                    let io = TokioIo::new(stream);

                    // 使用 HTTP/1.1 协议处理连接
                    // preserve_header_case: 保留请求头的原始大小写
                    // title_case_headers: 响应头使用首字母大写格式
                    if let Err(e) = http1::Builder::new()
                        .preserve_header_case(true)
                        .title_case_headers(true)
                        .serve_connection(io, service)  // 使用我们的代理服务处理请求
                        .await
                    {
                        // 连接处理错误（通常是客户端断开连接）
                        error!("Failed to serve connection: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

// ============================================================================
// 主函数
// ============================================================================

/// 程序入口点
///
/// 主函数负责：
/// 1. 初始化日志系统
/// 2. 读取并解析配置文件
/// 3. 启动所有配置的代理服务
/// 4. 等待所有代理服务运行
///
/// 使用 #[tokio::main] 属性宏设置 Tokio 异步运行时
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志系统
    // 可以通过 RUST_LOG 环境变量控制日志级别
    // 例如：RUST_LOG=info cargo run
    env_logger::init();

    // 读取配置文件
    // expect: 如果文件不存在，程序会 panic 并显示错误信息
    let config_str =
        std::fs::read_to_string("config.toml")
            .expect("Failed to read config.toml. Please create config.toml file first!");

    // 解析 TOML 配置为 Config 结构体
    let config: Config = toml::from_str(&config_str)
        .expect("Failed to parse config.toml");

    info!("Starting proxy server...");

    // 用于存储所有代理任务的句柄
    // 我们需要保持这些句柄，以便程序不会提前退出
    let mut handles = vec![];

    // ========================================================================
    // 启动 TCP 代理
    // ========================================================================
    for tcp_config in config.tcp_proxies.clone() {
        info!(
            "Starting TCP proxy: {} -> {}",
            tcp_config.listen_addr, tcp_config.backend_addr
        );

        // tokio::spawn 创建新的异步任务
        // 每个 TCP 代理在独立的异步任务中运行
        let handle = tokio::spawn(async move {
            if let Err(e) = run_tcp_proxy(tcp_config.listen_addr, tcp_config.backend_addr).await {
                error!("TCP proxy error: {}", e);
            }
        });
        handles.push(handle);
    }

    // ========================================================================
    // 启动 UDP 代理
    // ========================================================================
    for udp_config in config.udp_proxies.clone() {
        info!(
            "Starting UDP proxy: {} -> {}",
            udp_config.listen_addr, udp_config.backend_addr
        );

        let handle = tokio::spawn(async move {
            if let Err(e) = run_udp_proxy(udp_config.listen_addr, udp_config.backend_addr).await {
                error!("UDP proxy error: {}", e);
            }
        });
        handles.push(handle);
    }

    // ========================================================================
    // 启动 HTTP 代理（固定后端）
    // ========================================================================
    for http_config in config.http_proxies.clone() {
        info!(
            "Starting HTTP proxy: {} -> {}",
            http_config.listen_addr, http_config.backend_addr
        );

        let handle = tokio::spawn(async move {
            if let Err(e) =
                run_http_proxy(http_config.listen_addr, http_config.backend_addr).await
            {
                error!("HTTP proxy error: {}", e);
            }
        });
        handles.push(handle);
    }

    // ========================================================================
    // 启动 gRPC 动态代理
    // ========================================================================
    // gRPC 代理也使用 run_http_proxy 函数，但支持动态路由
    // 区别在于 ProxyService 的 extract_backend 方法会检查 x-backend 头部
    info!(
        "Starting gRPC dynamic proxy: {} -> {}",
        config.grpc_proxy.listen_addr, config.grpc_proxy.default_backend
    );

    let grpc_handle = tokio::spawn(async move {
        if let Err(e) =
            run_http_proxy(
                config.grpc_proxy.listen_addr,
                config.grpc_proxy.default_backend,
            ).await
        {
            error!("gRPC proxy error: {}", e);
        }
    });
    handles.push(grpc_handle);

    // 所有代理服务已启动
    info!("✅ All proxy servers started successfully!");

    // ========================================================================
    // 等待所有代理任务完成
    // ========================================================================
    // 注意：实际上代理任务是无限循环的，永远不会正常完成
    // 这个循环只是为了保持主函数不退出
    // 如果需要优雅关闭，可以监听 SIGTERM 信号
    for handle in handles {
        // handle.await 会等待异步任务完成
        // 如果任务 panic，这里会传播 panic
        let _ = handle.await;
    }

    // 这个 Ok(()) 实际上永远不会执行到
    // 因为上面的循环会永远等待
    Ok(())
}