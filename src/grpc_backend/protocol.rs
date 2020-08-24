use async_std::net::IpAddr;
use std::convert::TryFrom;
use std::net::SocketAddr;
use tonic::Status;

tonic::include_proto!("red.grpc_backend.protocol");

impl From<SocketAddr> for Address {
    fn from(socket_addr: SocketAddr) -> Self {
        let port = socket_addr.port();
        let host = match socket_addr.ip() {
            IpAddr::V4(ipv4) => ipv4.octets().to_vec(),
            IpAddr::V6(ipv6) => ipv6.octets().to_vec(),
        };

        Self {
            host,
            port: port as u32,
        }
    }
}

impl TryFrom<Address> for SocketAddr {
    type Error = tonic::Status;

    fn try_from(address: Address) -> Result<Self, Self::Error> {
        if address.port > u16::max_value() as u32 {
            return Err(Status::invalid_argument("invalid port"));
        }

        let ip: IpAddr = match address.host.len() {
            4 => {
                let mut octets: [u8; 4] = [0; 4];
                octets.copy_from_slice(&address.host[0..4]);
                Ok(octets.into())
            }
            16 => {
                let mut octets: [u8; 16] = [0; 16];
                octets.copy_from_slice(&address.host[0..16]);
                Ok(octets.into())
            }
            _ => Err(Status::invalid_argument(
                "host address must be either 4 or 16 bytes long",
            )),
        }?;

        let port = address.port as u16;
        Ok((ip, port).into())
    }
}
