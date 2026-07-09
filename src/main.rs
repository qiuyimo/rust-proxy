use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper_util::rt::TokioIo;
use log::{error, info, warn};
use serde::Deserialize;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream, UdpSocket};

// ========== 配置结构体 ==========

#[derive(Debug, Deserialize, Clone)]
struct TcpProxyConfig {
    listen_addr: String,
    backend_addr: String,
}

#[derive(Debug, Deserialize, Clone)]
struct UdpProxyConfig {
    listen_addr: String,
    backend_addr: String,
}

#[derive(Debug, Deserialize, Clone)]
struct HttpProxyConfig {
    listen_addr: String,
    backend_addr: String,
}

#[derive(Debug, Deserialize, Clone)]
struct GrpcProxyConfig {
    listen_addr: String,
    default_backend: String,
}

#[derive(Debug, Deserialize, Clone)]
struct Config {
    tcp_proxies: Vec<TcpProxyConfig>,
    udp_proxies: Vec<UdpProxyConfig>,
    http_proxies: Vec<HttpProxyConfig>,
    grpc_proxy: GrpcProxyConfig,
}

// ========== TCP 代理 ==========

async fn run_tcp_proxy(listen_addr: String, backend_addr: String) -> Result<()> {
    let listener = TcpListener::bind(&listen_addr).await?;
    info!("TCP proxy listening on {}", listen_addr);

    loop {
        match listener.accept().await {
            Ok((mut client_stream, client_addr)) => {
                let backend = backend_addr.clone();
                tokio::spawn(async move {
                    info!("TCP proxy: {} -> {}", client_addr, backend);
                    match TcpStream::connect(&backend).await {
                        Ok(mut backend_stream) => {
                            let (mut client_read, mut client_write) = client_stream.split();
                            let (mut backend_read, mut backend_write) = backend_stream.split();

                            let client_to_backend =
                                tokio::io::copy(&mut client_read, &mut backend_write);
                            let backend_to_client =
                                tokio::io::copy(&mut backend_read, &mut client_write);

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
                            error!("Failed to connect to backend {}: {}", backend, e);
                        }
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept TCP connection: {}", e);
            }
        }
    }
}

// ========== UDP 代理 ==========

async fn run_udp_proxy(listen_addr: String, backend_addr: String) -> Result<()> {
    let socket = UdpSocket::bind(&listen_addr).await?;
    info!("UDP proxy listening on {}", listen_addr);
    let socket = Arc::new(socket);
    let mut buf = vec![0u8; 65535];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, client_addr)) => {
                let data = buf[..size].to_vec();
                let backend = backend_addr.clone();
                let socket_clone = socket.clone();

                tokio::spawn(async move {
                    let backend_socket = match UdpSocket::bind("0.0.0.0:0").await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to create UDP socket: {}", e);
                            return;
                        }
                    };

                    if let Err(e) = backend_socket.send_to(&data, &backend).await {
                        error!("Failed to send to backend: {}", e);
                        return;
                    }

                    let mut recv_buf = vec![0u8; 65535];
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        backend_socket.recv_from(&mut recv_buf),
                    )
                        .await
                    {
                        Ok(Ok((recv_size, _))) => {
                            if let Err(e) = socket_clone
                                .send_to(&recv_buf[..recv_size], client_addr)
                                .await
                            {
                                error!("Failed to send to client: {}", e);
                            }
                        }
                        Ok(Err(e)) => {
                            error!("Failed to receive from backend: {}", e);
                        }
                        Err(_) => {
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

// ========== HTTP/gRPC 代理服务 ==========

#[derive(Clone)]
struct ProxyService {
    default_backend: String,
    client: hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        Incoming,
    >,
}

impl ProxyService {
    fn new(default_backend: String) -> Self {
        let connector = hyper_util::client::legacy::connect::HttpConnector::new();
        let client: hyper_util::client::legacy::Client<
            hyper_util::client::legacy::connect::HttpConnector,
            Incoming,
        > = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build(connector);

        ProxyService {
            default_backend,
            client,
        }
    }

    fn extract_backend(&self, req: &Request<Incoming>) -> String {
        // 检查 x-backend 头部
        if let Some(backend_value) = req.headers().get("x-backend") {
            if let Ok(backend_str) = backend_value.to_str() {
                info!("Dynamic gRPC proxy to: {}", backend_str);
                return backend_str.to_string();
            }
        }

        // 如果没有 x-backend，使用默认后端
        self.default_backend.clone()
    }
}

impl Service<Request<Incoming>> for ProxyService {
    type Response = Response<BoxBody<Bytes, hyper::Error>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, mut req: Request<Incoming>) -> Self::Future {
        let backend = self.extract_backend(&req);
        let client = self.client.clone();

        // 构建后端 URI
        let path_and_query = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let uri = format!("http://{}{}", backend, path_and_query);

        info!("Forwarding request to: {}", uri);

        // 移除 x-backend 头部，避免传递给后端
        req.headers_mut().remove("x-backend");

        Box::pin(async move {
            // 构建后端请求
            let mut backend_req = Request::builder().method(req.method().clone()).uri(&uri);

            // 复制头部（排除 host 和 x-backend）
            for (key, value) in req.headers().iter() {
                if key.as_str() != "host" && key.as_str() != "x-backend" {
                    if let Ok(v) = value.to_str() {
                        backend_req = backend_req.header(key.as_str(), v);
                    }
                }
            }

            // 设置 Host 头部
            if let Some(host) = req.uri().host() {
                backend_req = backend_req.header("host", host);
            }

            let backend_req = match backend_req.body(req.into_body()) {
                Ok(r) => r,
                Err(e) => {
                    error!("Failed to build backend request: {}", e);
                    let response = Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(
                            Full::new(Bytes::from(format!("Bad request: {}", e)))
                                .map_err(|never| match never {})
                                .boxed(),
                        )
                        .unwrap();
                    return Ok(response);
                }
            };

            // 转发请求到后端
            match client.request(backend_req).await {
                Ok(response) => {
                    info!("Backend response status: {}", response.status());
                    let (parts, body) = response.into_parts();
                    let boxed_body = body.map_err(|e| e).boxed();
                    Ok(Response::from_parts(parts, boxed_body))
                }
                Err(e) => {
                    error!("Failed to connect to backend: {}", e);
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

async fn run_http_proxy(listen_addr: String, backend_addr: String) -> Result<()> {
    let addr: SocketAddr = listen_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("HTTP/gRPC proxy listening on {}", listen_addr);

    let proxy_service = ProxyService::new(backend_addr);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let service = proxy_service.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    if let Err(e) = http1::Builder::new()
                        .preserve_header_case(true)
                        .title_case_headers(true)
                        .serve_connection(io, service)
                        .await
                    {
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

// ========== 主函数 ==========

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // 读取配置
    let config_str =
        std::fs::read_to_string("config.toml").expect("Failed to read config.toml. Please create config.toml file first!");
    let config: Config = toml::from_str(&config_str).expect("Failed to parse config.toml");

    info!("Starting proxy server...");

    let mut handles = vec![];

    // 启动 TCP 代理
    for tcp_config in config.tcp_proxies.clone() {
        info!(
            "Starting TCP proxy: {} -> {}",
            tcp_config.listen_addr, tcp_config.backend_addr
        );
        let handle = tokio::spawn(async move {
            if let Err(e) = run_tcp_proxy(tcp_config.listen_addr, tcp_config.backend_addr).await {
                error!("TCP proxy error: {}", e);
            }
        });
        handles.push(handle);
    }

    // 启动 UDP 代理
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

    // 启动 HTTP 代理
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

    // 启动 gRPC 动态代理
    info!(
        "Starting gRPC dynamic proxy: {} -> {}",
        config.grpc_proxy.listen_addr, config.grpc_proxy.default_backend
    );
    let grpc_handle = tokio::spawn(async move {
        if let Err(e) =
            run_http_proxy(config.grpc_proxy.listen_addr, config.grpc_proxy.default_backend).await
        {
            error!("gRPC proxy error: {}", e);
        }
    });
    handles.push(grpc_handle);

    info!("✅ All proxy servers started successfully!");

    // 等待所有任务完成
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}