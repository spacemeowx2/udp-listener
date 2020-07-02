#![warn(missing_debug_implementations, rust_2018_idioms)]

use async_channel::{unbounded, Receiver, Sender, TrySendError};
use async_mutex::Mutex;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::fmt;
use tokio::net::{udp, ToSocketAddrs, UdpSocket};

type Packet = Vec<u8>;

fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

struct Inner {
    sender: Sender<UdpStream>,
    rx: Mutex<udp::RecvHalf>,
    tx: Mutex<udp::SendHalf>,
    children: Mutex<HashMap<SocketAddr, Sender<Packet>>>,
}

impl Inner {
    async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
        self.tx.lock().await.send_to(buf, target).await
    }
    async fn serve(self: Arc<Inner>) -> io::Result<()> {
        let socket = &mut self.rx.lock().await;
        loop {
            let mut buf = vec![0u8; 65536];
            let (size, addr) = socket.recv_from(&mut buf).await?;
            buf.truncate(size);

            let mut children = self.children.lock().await;
            let sender = match children.get(&addr) {
                Some(sender) => sender.clone(),
                None => {
                    let (tx, rx) = unbounded();
                    let stream = UdpStream::new(self.clone(), addr, rx);
                    children.insert(addr, tx.clone());
                    self.sender.try_send(stream).map_err(other)?;
                    tx
                }
            };
            match sender.try_send(buf) {
                Ok(_) => {}
                Err(TrySendError::Closed(_)) => {
                    children.remove(&addr);
                }
                _ => unreachable!(),
            };
        }
    }
}

pub struct SendHalf {
    inner: Arc<Inner>,
    target: SocketAddr,
}

impl fmt::Debug for SendHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendHalf")
            .field("target", &self.target)
            .finish()
    }
}

#[derive(Debug)]
pub struct RecvHalf {
    receiver: Receiver<Packet>,
}

impl SendHalf {
    pub async fn send(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.send_to(buf, &self.target).await
    }
}

impl RecvHalf {
    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let p = self.receiver.recv().await.map_err(other)?;
        let len = std::cmp::min(buf.len(), p.len());
        buf.copy_from_slice(&p[..len]);
        Ok(len)
    }
}

pub struct UdpStream {
    tx: SendHalf,
    rx: RecvHalf,
}

impl fmt::Debug for UdpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdpStream")
            .field("target", &self.tx.target)
            .finish()
    }
}

impl UdpStream {
    fn new(inner: Arc<Inner>, target: SocketAddr, receiver: Receiver<Packet>) -> UdpStream {
        UdpStream {
            tx: SendHalf { inner, target },
            rx: RecvHalf { receiver },
        }
    }
    pub async fn send(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tx.send(buf).await
    }
    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.rx.recv(buf).await
    }
    pub fn split(self) -> (RecvHalf, SendHalf) {
        (self.rx, self.tx)
    }
}


pub struct UdpListener {
    receiver: Receiver<UdpStream>,
}

impl fmt::Debug for UdpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdpListener")
            .finish()
    }
}

impl UdpListener {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<UdpListener> {
        Self::from_tokio(UdpSocket::bind(addr).await?)
    }
    pub fn from_tokio(udp: UdpSocket) -> io::Result<UdpListener> {
        let (rx, tx) = udp.split();
        let (sender, receiver) = unbounded();
        let inner = Arc::new(Inner {
            sender,
            rx: Mutex::new(rx),
            tx: Mutex::new(tx),
            children: Mutex::new(HashMap::new()),
        });
        tokio::spawn(inner.clone().serve());
        Ok(UdpListener { receiver })
    }
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<UdpListener> {
        Self::from_tokio(UdpSocket::from_std(socket)?)
    }
    pub async fn next(&mut self) -> io::Result<UdpStream> {
        self.receiver.recv().await.map_err(other)
    }
}
