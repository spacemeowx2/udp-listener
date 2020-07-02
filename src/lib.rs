use tokio::net::{ToSocketAddrs, UdpSocket, udp::{SendHalf, RecvHalf}};
use std::io;
use std::sync::{Arc};
use std::net::SocketAddr;
use std::collections::HashMap;
use async_mutex::Mutex;
use async_channel::{unbounded, Sender, Receiver, TrySendError};

type Packet = Vec<u8>;

fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

struct Inner {
    sender: Sender<UdpStream>,
    rx: Mutex<RecvHalf>,
    tx: Mutex<SendHalf>,
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
                Some(sender) => {
                    sender.clone()
                }
                None => {
                    let (tx, rx) = unbounded();
                    let stream = UdpStream {
                        receiver: rx,
                        inner: self.clone(),
                        target: addr,
                    };
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
                _ => unreachable!()
            };
        }
    }
}

pub struct UdpStream {
    receiver: Receiver<Packet>,
    inner: Arc<Inner>,
    target: SocketAddr,
}

impl UdpStream {
    pub async fn send(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.send_to(buf, &self.target).await
    }
    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let p = self.receiver.recv().await.map_err(other)?;
        let len = std::cmp::min(buf.len(), p.len());
        buf.copy_from_slice(&p[..len]);
        Ok(len)
    }
}

pub struct UdpListener {
    receiver: Receiver<UdpStream>,
}

impl UdpListener {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<UdpListener> {
        let (rx, tx) = UdpSocket::bind(addr).await?.split();
        let (sender, receiver) = unbounded();
        let inner = Arc::new(Inner {
            sender,
            rx: Mutex::new(rx),
            tx: Mutex::new(tx),
            children: Mutex::new(HashMap::new()),
        });
        tokio::spawn(inner.clone().serve());
        Ok(UdpListener {
            receiver,
        })
    }
    pub async fn next(&mut self) -> io::Result<UdpStream> {
        self.receiver.recv().await.map_err(other)
    }
}
