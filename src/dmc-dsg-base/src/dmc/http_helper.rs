use async_std::net::TcpStream;
use cyfs_base::*;
use serde::Deserialize;
use tide::http::{Method, Mime, Request, Url};
use crate::app_msg;

#[allow(unused)]
pub async fn http_get_request(url: &str) -> BuckyResult<Vec<u8>> {
    let url_obj = Url::parse(url).unwrap();
    let host = url_obj.host().unwrap().to_string();
    let mut port = 80;
    if url_obj.port().is_some() {
        port = url_obj.port().unwrap();
    }
    let req = Request::new(Method::Get, url_obj);
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(addr).await.map_err(|err| {
        let msg = app_msg!("connect to failed! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    let mut resp = async_h1::connect(stream, req).await.map_err(|err| {
        let msg = app_msg!("http connect error! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    resp.body_bytes().await.map_err(|err| {
        let msg = app_msg!("recv body error! err={}", err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::InvalidData, msg)
    })
}

pub async fn http_post_request(url: &str, param: &[u8], content_type: Option<&str>) -> BuckyResult<Vec<u8>> {
    let url_obj = Url::parse(url).unwrap();
    let host = url_obj.host().unwrap().to_string();
    let mut port = 80;
    if url_obj.port().is_some() {
        port = url_obj.port().unwrap();
    }
    let mut req = Request::new(Method::Post, url_obj);
    if content_type.is_some() {
        req.set_content_type(Mime::from(content_type.unwrap()));
    }
    req.set_body(param);
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(addr).await.map_err(|err| {
        let msg = app_msg!("connect to failed! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    let mut resp = async_h1::connect(stream, req).await.map_err(|err| {
        let msg = app_msg!("http connect error! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    resp.body_bytes().await.map_err(|err| {
        let msg = app_msg!("recv body error! err={}", err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::InvalidData, msg)
    })
}

pub async fn http_post_request3<T: for<'de> Deserialize<'de>>(url: &str, param: &[u8], content_type: Option<&str>) -> BuckyResult<T> {
    let url_obj = Url::parse(url).unwrap();
    let host = url_obj.host().unwrap().to_string();
    let mut port = 80;
    if url_obj.port().is_some() {
        port = url_obj.port().unwrap();
    }
    let mut req = Request::new(Method::Post, url_obj);
    if content_type.is_some() {
        req.set_content_type(Mime::from(content_type.unwrap()));
    }
    req.set_body(param);
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(addr).await.map_err(|err| {
        let msg = app_msg!("connect to failed! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    let mut resp = async_h1::connect(stream, req).await.map_err(|err| {
        let msg = app_msg!("http connect error! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    resp.body_json().await.map_err(|err| {
        let msg = app_msg!("recv body error! err={}", err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::InvalidData, msg)
    })
}

pub async fn http_post_json(url: &str, param: json::JsonValue) -> BuckyResult<json::JsonValue> {
    let url_obj = Url::parse(url).unwrap();
    let host = url_obj.host().unwrap().to_string();
    let mut port = 80;
    if url_obj.port().is_some() {
        port = url_obj.port().unwrap();
    }

    let mut req = Request::new(Method::Post, url_obj);
    req.set_content_type(Mime::from("application/json"));
    req.set_body(param.to_string());
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(addr).await.map_err(|err| {
        let msg = app_msg!("connect to failed! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    let mut resp = async_h1::connect(stream, req).await.map_err(|err| {
        let msg = app_msg!("http connect error! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    let resp_str = resp.body_string().await.map_err(|err| {
        let msg = app_msg!("recv body error! err={}", err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    json::parse(resp_str.as_str()).map_err(|err| {
        let msg = app_msg!("parse {} error! err={}", resp_str.as_str(), err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::InvalidData, msg)
    })
}


pub async fn http_post_json2<T: for<'de> Deserialize<'de>>(url: &str, param: json::JsonValue) -> BuckyResult<T> {
    let url_obj = Url::parse(url).unwrap();
    let host = url_obj.host().unwrap().to_string();
    let mut port = 80;
    if url_obj.port().is_some() {
        port = url_obj.port().unwrap();
    }
    let mut req = Request::new(Method::Post, url_obj);
    req.set_content_type(Mime::from("application/json"));
    req.set_body(param.to_string());
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(addr).await.map_err(|err| {
        let msg = app_msg!("connect to failed! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    let mut resp = async_h1::connect(stream, req).await.map_err(|err| {
        let msg = app_msg!("http connect error! host={}, err={}", host, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::ConnectFailed, msg)
    })?;

    let tx = resp.body_string().await.unwrap();
    serde_json::from_str(tx.as_str()).map_err(|err| {
        let msg = app_msg!("recv {} error! err={}", tx, err);
        log::error!("{}", msg.as_str());
        BuckyError::new(BuckyErrorCode::InvalidData, msg)
    })
}
