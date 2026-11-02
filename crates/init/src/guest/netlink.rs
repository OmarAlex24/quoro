// SPDX-License-Identifier: AGPL-3.0-only
//! The guest network, configured over rtnetlink.
//!
//! `rustix` has no netlink message layer, so the messages are built and parsed
//! here as bytes. The builders and the reply parser are pure and unit-tested;
//! only [`configure`] touches a socket. Netlink is host-endian, so every field
//! is written with `to_ne_bytes`.

use std::net::Ipv4Addr;
use std::os::fd::OwnedFd;
use std::thread;
use std::time::{Duration, Instant};

use quoro_init::NetConfig;
use rustix::net::netlink::SocketAddrNetlink;
use rustix::net::sockopt::{Timeout, set_socket_timeout};
use rustix::net::{AddressFamily, RecvFlags, SendFlags, SocketType, bind, recv, sendto, socket};

use crate::guest::Fault;

// Types and flags from `linux/rtnetlink.h`, `linux/netlink.h` and
// `linux/if_addr.h`.
const NLMSG_ERROR: u16 = 2;
const NLM_F_REQUEST: u16 = 0x0001;
const NLM_F_ACK: u16 = 0x0004;
const NLM_F_EXCL: u16 = 0x0200;
const NLM_F_CREATE: u16 = 0x0400;
const RTM_NEWLINK: u16 = 16;
const RTM_NEWADDR: u16 = 20;
const RTM_NEWROUTE: u16 = 24;
const IFA_ADDRESS: u16 = 1;
const IFA_LOCAL: u16 = 2;
const RTA_OIF: u16 = 4;
const RTA_GATEWAY: u16 = 5;
const AF_UNSPEC: u8 = 0;
const AF_INET: u8 = 2;
const IFF_UP: u32 = 1;
const RT_TABLE_MAIN: u8 = 254;
const RTPROT_BOOT: u8 = 3;
const RT_SCOPE_UNIVERSE: u8 = 0;
const RTN_UNICAST: u8 = 1;
/// `EEXIST`: the address or route is already there, which is not a failure.
const EEXIST: i32 = 17;

/// The size of a `struct nlmsghdr`.
const NLMSG_HEADER_LEN: usize = 16;
/// How long to wait for the interface with the configured MAC.
const LINK_WAIT: Duration = Duration::from_millis(5000);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Round a netlink length up to the next 4-byte boundary.
fn align4(len: usize) -> usize {
    (len + 3) & !3
}

fn read_u16(buf: &[u8], offset: usize) -> Option<u16> {
    let end = offset.checked_add(2)?;
    let bytes = buf.get(offset..end)?;
    Some(u16::from_ne_bytes([bytes[0], bytes[1]]))
}

fn read_u32(buf: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let bytes = buf.get(offset..end)?;
    Some(u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_i32(buf: &[u8], offset: usize) -> Option<i32> {
    read_u32(buf, offset).map(|value| value as i32)
}

/// One `struct rtattr` — length, type, payload, padded to four bytes.
fn attr(kind: u16, payload: &[u8]) -> Vec<u8> {
    let len = 4 + payload.len();
    let mut out = Vec::with_capacity(align4(len));
    out.extend_from_slice(&(u16::try_from(len).unwrap_or(u16::MAX)).to_ne_bytes());
    out.extend_from_slice(&kind.to_ne_bytes());
    out.extend_from_slice(payload);
    out.resize(align4(len), 0);
    out
}

/// One `struct nlmsghdr` followed by `body`.
fn message(kind: u16, flags: u16, seq: u32, body: &[u8]) -> Vec<u8> {
    let len = NLMSG_HEADER_LEN + body.len();
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&(u32::try_from(len).unwrap_or(u32::MAX)).to_ne_bytes());
    out.extend_from_slice(&kind.to_ne_bytes());
    out.extend_from_slice(&flags.to_ne_bytes());
    out.extend_from_slice(&seq.to_ne_bytes());
    // nlmsg_pid: the kernel fills it in for requests.
    out.extend_from_slice(&0u32.to_ne_bytes());
    out.extend_from_slice(body);
    out
}

/// `RTM_NEWLINK` that sets `IFF_UP` on `ifindex`.
fn link_up(ifindex: u32, seq: u32) -> Vec<u8> {
    let mut body = Vec::with_capacity(16);
    body.push(AF_UNSPEC);
    body.push(0); // ifi_pad
    body.extend_from_slice(&0u16.to_ne_bytes()); // ifi_type
    body.extend_from_slice(&(ifindex as i32).to_ne_bytes());
    body.extend_from_slice(&IFF_UP.to_ne_bytes()); // ifi_flags
    body.extend_from_slice(&IFF_UP.to_ne_bytes()); // ifi_change
    message(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK, seq, &body)
}

/// `RTM_NEWADDR` for `address/prefix` on `ifindex`, as `IFA_LOCAL` and
/// `IFA_ADDRESS` at once (a host address, not a peer).
fn add_addr(ifindex: u32, address: Ipv4Addr, prefix_len: u8, seq: u32) -> Vec<u8> {
    let mut body = Vec::with_capacity(8);
    body.push(AF_INET);
    body.push(prefix_len);
    body.push(0); // ifa_flags
    body.push(RT_SCOPE_UNIVERSE); // ifa_scope
    body.extend_from_slice(&ifindex.to_ne_bytes());
    body.extend_from_slice(&attr(IFA_LOCAL, &address.octets()));
    body.extend_from_slice(&attr(IFA_ADDRESS, &address.octets()));
    message(
        RTM_NEWADDR,
        NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL,
        seq,
        &body,
    )
}

/// `RTM_NEWROUTE`: the default route (no `RTA_DST`, `dst_len` 0) via `gateway`
/// out of `ifindex`, in the main table.
fn default_route(ifindex: u32, gateway: Ipv4Addr, seq: u32) -> Vec<u8> {
    let mut body = Vec::with_capacity(12);
    body.push(AF_INET);
    body.push(0); // rtm_dst_len: the default route
    body.push(0); // rtm_src_len
    body.push(0); // rtm_tos
    body.push(RT_TABLE_MAIN);
    body.push(RTPROT_BOOT);
    body.push(RT_SCOPE_UNIVERSE);
    body.push(RTN_UNICAST);
    body.extend_from_slice(&0u32.to_ne_bytes()); // rtm_flags
    body.extend_from_slice(&attr(RTA_GATEWAY, &gateway.octets()));
    body.extend_from_slice(&attr(RTA_OIF, &ifindex.to_ne_bytes()));
    message(
        RTM_NEWROUTE,
        NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL,
        seq,
        &body,
    )
}

/// What a reply message means for the request that caused it.
#[derive(Debug, PartialEq, Eq)]
enum Reply {
    /// The kernel acknowledged the request.
    Ack,
    /// The object already existed; treated as success.
    Exists,
    /// The kernel refused the request.
    Failed(String),
}

/// Find the reply to request `seq` in one datagram. `NLMSG_ERROR` with a zero
/// code is an acknowledgement; a negative code is an errno.
fn parse_reply(buf: &[u8], seq: u32) -> Option<Reply> {
    let mut offset = 0usize;
    while offset + NLMSG_HEADER_LEN <= buf.len() {
        let len = read_u32(buf, offset)? as usize;
        if len < NLMSG_HEADER_LEN {
            return None;
        }
        let kind = read_u16(buf, offset + 4)?;
        let message_seq = read_u32(buf, offset + 8)?;
        let end = offset.checked_add(len)?;
        if end > buf.len() {
            return None;
        }
        if kind == NLMSG_ERROR && message_seq == seq {
            let code = read_i32(buf, offset + NLMSG_HEADER_LEN)?;
            return Some(if code == 0 {
                Reply::Ack
            } else if code == -EEXIST {
                Reply::Exists
            } else {
                Reply::Failed(format!("netlink error {code}"))
            });
        }
        offset = align4(end);
    }
    None
}

/// A bound `NETLINK_ROUTE` socket that numbers its own requests.
struct RouteSocket {
    fd: OwnedFd,
    seq: u32,
}

impl RouteSocket {
    fn open() -> Result<Self, Fault> {
        // NETLINK_ROUTE is protocol 0.
        let fd = socket(AddressFamily::NETLINK, SocketType::RAW, None)
            .map_err(|err| Fault::new(format!("netlink socket: {err}")))?;
        bind(&fd, &SocketAddrNetlink::new(0, 0))
            .map_err(|err| Fault::new(format!("netlink bind: {err}")))?;
        // Never block the boot on a reply the kernel does not send.
        set_socket_timeout(&fd, Timeout::Recv, Some(Duration::from_secs(2)))
            .map_err(|err| Fault::new(format!("netlink timeout: {err}")))?;
        Ok(Self { fd, seq: 0 })
    }

    /// Send one request and wait for its acknowledgement.
    fn request(&mut self, build: impl FnOnce(u32) -> Vec<u8>) -> Result<(), Fault> {
        self.seq = self.seq.wrapping_add(1);
        let request = build(self.seq);
        sendto(
            &self.fd,
            &request,
            SendFlags::empty(),
            &SocketAddrNetlink::new(0, 0),
        )
        .map_err(|err| Fault::new(format!("netlink send: {err}")))?;

        let mut buf = [0u8; 4096];
        for _ in 0..16 {
            let (read, _total) = recv(&self.fd, &mut buf[..], RecvFlags::empty())
                .map_err(|err| Fault::new(format!("netlink recv: {err}")))?;
            let usable = read.min(buf.len());
            match parse_reply(&buf[..usable], self.seq) {
                Some(Reply::Ack | Reply::Exists) => return Ok(()),
                Some(Reply::Failed(text)) => return Err(Fault::new(text)),
                None => continue,
            }
        }
        Err(Fault::new("netlink: no acknowledgement"))
    }

    fn link_up(&mut self, ifindex: u32) -> Result<(), Fault> {
        self.request(|seq| link_up(ifindex, seq))
    }

    fn add_addr(&mut self, ifindex: u32, address: Ipv4Addr, prefix_len: u8) -> Result<(), Fault> {
        self.request(|seq| add_addr(ifindex, address, prefix_len, seq))
    }

    fn default_route(&mut self, ifindex: u32, gateway: Ipv4Addr) -> Result<(), Fault> {
        self.request(|seq| default_route(ifindex, gateway, seq))
    }
}

/// `02:00:00:00:00:01`, the form `/sys/class/net/<if>/address` uses.
fn format_mac(mac: [u8; 6]) -> String {
    mac.iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// The ifindex of `name`, from sysfs.
fn ifindex(name: &str) -> Option<u32> {
    std::fs::read_to_string(format!("/sys/class/net/{name}/ifindex"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// The ifindex of the interface whose hardware address is `mac`, waiting up to
/// [`LINK_WAIT`] for the kernel to create it.
fn wait_for_interface(mac: [u8; 6]) -> Option<u32> {
    let wanted = format_mac(mac);
    let deadline = Instant::now() + LINK_WAIT;
    loop {
        if let Ok(entries) = std::fs::read_dir("/sys/class/net") {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                let Ok(address) = std::fs::read_to_string(entry.path().join("address")) else {
                    continue;
                };
                if address.trim().eq_ignore_ascii_case(&wanted)
                    && let Some(index) = ifindex(name)
                {
                    return Some(index);
                }
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Step 7: loopback up, address on the interface with the configured MAC, link
/// up, default route.
pub(crate) fn configure(net: &NetConfig) -> Result<(), Fault> {
    let mut socket = RouteSocket::open()?;

    let loopback = ifindex("lo").ok_or_else(|| Fault::new("no loopback interface"))?;
    socket.link_up(loopback)?;

    let link = wait_for_interface(net.mac).ok_or_else(|| {
        Fault::new(format!(
            "no interface with mac {} after {} ms",
            format_mac(net.mac),
            LINK_WAIT.as_millis()
        ))
    })?;
    socket.add_addr(link, net.address, net.prefix_len)?;
    socket.link_up(link)?;
    socket.default_route(link, net.gateway)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Read a `nlmsghdr`'s fields back out of a built message.
    fn header(message: &[u8]) -> (u32, u16, u16, u32) {
        (
            read_u32(message, 0).unwrap_or(0),
            read_u16(message, 4).unwrap_or(0),
            read_u16(message, 6).unwrap_or(0),
            read_u32(message, 8).unwrap_or(0),
        )
    }

    /// Walk the attributes of a body that starts at `offset`, returning
    /// `(type, payload)`.
    fn attrs(message: &[u8], mut offset: usize) -> Vec<(u16, Vec<u8>)> {
        let mut found = Vec::new();
        while offset + 4 <= message.len() {
            let len = read_u16(message, offset).unwrap_or(0) as usize;
            if len < 4 || offset + len > message.len() {
                break;
            }
            let kind = read_u16(message, offset + 2).unwrap_or(0);
            found.push((kind, message[offset + 4..offset + len].to_vec()));
            offset += align4(len);
        }
        found
    }

    #[test]
    fn link_up_message_is_well_formed() {
        let message = link_up(7, 3);
        assert_eq!(message.len(), 32);
        let (len, kind, flags, seq) = header(&message);
        assert_eq!(len as usize, message.len());
        assert_eq!(kind, RTM_NEWLINK);
        assert_eq!(flags, NLM_F_REQUEST | NLM_F_ACK);
        assert_eq!(seq, 3);
        // `struct ifinfomsg` starts at the end of the header.
        assert_eq!(message[16], AF_UNSPEC);
        assert_eq!(read_u32(&message, 20), Some(7));
        assert_eq!(read_u32(&message, 24), Some(IFF_UP));
        assert_eq!(read_u32(&message, 28), Some(IFF_UP));
    }

    #[test]
    fn add_addr_carries_local_and_peer_attributes() {
        let message = add_addr(2, Ipv4Addr::new(10, 99, 1, 2), 24, 9);
        let (len, kind, flags, seq) = header(&message);
        assert_eq!(len as usize, message.len());
        assert_eq!(kind, RTM_NEWADDR);
        assert_eq!(flags, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL);
        assert_eq!(seq, 9);
        assert_eq!(message[16], AF_INET);
        assert_eq!(message[17], 24);
        assert_eq!(read_u32(&message, 20), Some(2));
        let attributes = attrs(&message, 24);
        assert_eq!(
            attributes,
            vec![
                (IFA_LOCAL, vec![10, 99, 1, 2]),
                (IFA_ADDRESS, vec![10, 99, 1, 2]),
            ]
        );
    }

    #[test]
    fn default_route_message_is_well_formed() {
        let message = default_route(2, Ipv4Addr::new(10, 99, 1, 1), 11);
        let (len, kind, flags, seq) = header(&message);
        assert_eq!(len as usize, message.len());
        assert_eq!(kind, RTM_NEWROUTE);
        assert_eq!(flags, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL);
        assert_eq!(seq, 11);
        assert_eq!(message[16], AF_INET);
        assert_eq!(message[17], 0, "default route has no destination prefix");
        assert_eq!(message[20], RT_TABLE_MAIN);
        assert_eq!(message[21], RTPROT_BOOT);
        assert_eq!(message[22], RT_SCOPE_UNIVERSE);
        assert_eq!(message[23], RTN_UNICAST);
        let attributes = attrs(&message, 28);
        assert_eq!(
            attributes,
            vec![
                (RTA_GATEWAY, vec![10, 99, 1, 1]),
                (RTA_OIF, 2u32.to_ne_bytes().to_vec()),
            ]
        );
    }

    /// An `NLMSG_ERROR` reply carrying `code`.
    fn error_reply(seq: u32, code: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&code.to_ne_bytes());
        body.resize(20, 0);
        message(NLMSG_ERROR, 0, seq, &body)
    }

    #[test]
    fn acknowledgements_and_errors_are_told_apart() {
        assert_eq!(parse_reply(&error_reply(5, 0), 5), Some(Reply::Ack));
        assert_eq!(
            parse_reply(&error_reply(5, -EEXIST), 5),
            Some(Reply::Exists)
        );
        assert_eq!(
            parse_reply(&error_reply(5, -13), 5),
            Some(Reply::Failed("netlink error -13".to_string()))
        );
    }

    #[test]
    fn replies_for_other_requests_are_ignored() {
        assert_eq!(parse_reply(&error_reply(5, 0), 6), None);
    }

    #[test]
    fn truncated_replies_are_ignored_not_panics() {
        let full = error_reply(5, 0);
        for cut in 0..full.len() {
            assert_eq!(parse_reply(&full[..cut], 5), None);
        }
    }

    #[test]
    fn several_messages_in_one_datagram_are_walked() {
        let mut buffer = error_reply(4, 0);
        buffer.extend_from_slice(&error_reply(5, -EEXIST));
        assert_eq!(parse_reply(&buffer, 5), Some(Reply::Exists));
        assert_eq!(parse_reply(&buffer, 4), Some(Reply::Ack));
    }

    #[test]
    fn mac_formatting_matches_sysfs() {
        assert_eq!(
            format_mac([0x02, 0x00, 0x00, 0x00, 0x99, 0x0a]),
            "02:00:00:00:99:0a"
        );
    }
}
