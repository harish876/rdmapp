#include "acceptor.h"

#include "qp_transmission.h"
#include "socket/channel.h"
#include "socket/tcp_connection.h"
#include "socket/tcp_listener.h"
#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <strings.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include <rdmapp/detail/debug.h>
#include <rdmapp/device.h>
#include <rdmapp/error.h>
#include <rdmapp/qp.h>
#include <rdmapp/srq.h>
#include <rdmapp/detail/serdes.h>

namespace rdmapp {

acceptor::acceptor(std::shared_ptr<socket::event_loop> loop, uint16_t port,
                   std::shared_ptr<pd> pd, std::shared_ptr<cq> cq,
                   std::shared_ptr<srq> srq, enum ibv_qp_type qp_type)
    : acceptor(loop, port, pd, cq, cq, srq, qp_type) {}

acceptor::acceptor(std::shared_ptr<socket::event_loop> loop, uint16_t port,
                   std::shared_ptr<pd> pd, std::shared_ptr<cq> recv_cq,
                   std::shared_ptr<cq> send_cq, std::shared_ptr<srq> srq,
                   enum ibv_qp_type qp_type)
    : acceptor(loop, "", port, pd, recv_cq, send_cq, srq, qp_type) {}

acceptor::acceptor(std::shared_ptr<socket::event_loop> loop,
                   std::string const &hostname, uint16_t port,
                   std::shared_ptr<pd> pd, std::shared_ptr<cq> cq,
                   std::shared_ptr<srq> srq, enum ibv_qp_type qp_type)
    : acceptor(loop, hostname, port, pd, cq, cq, srq, qp_type) {}

acceptor::acceptor(std::shared_ptr<socket::event_loop> loop,
                   std::string const &hostname, uint16_t port,
                   std::shared_ptr<pd> pd, std::shared_ptr<cq> recv_cq,
                   std::shared_ptr<cq> send_cq, std::shared_ptr<srq> srq,
                   enum ibv_qp_type qp_type)
    : listener_(std::make_unique<socket::tcp_listener>(loop, hostname, port)),
      pd_(pd), recv_cq_(recv_cq), send_cq_(send_cq), srq_(srq),
      qp_type_(qp_type) {}

task<std::shared_ptr<qp>> acceptor::accept() {
  auto channel = co_await listener_->accept();
  auto connection = socket::tcp_connection(channel);
  auto remote_qp = co_await recv_qp(connection);
  auto local_qp = std::make_shared<qp>(
      remote_qp.header.lid, remote_qp.header.qp_num, remote_qp.header.sq_psn,
      remote_qp.header.gid, pd_, recv_cq_, send_cq_, qp_type_);
  local_qp->user_data() = std::move(remote_qp.user_data);
  user_data_ = local_qp->user_data();
  co_await send_qp(*local_qp, connection);
  co_return local_qp;
}

const std::vector<uint8_t> &acceptor::get_user_data() const {
  return user_data_;
}

std::vector<uint8_t> &acceptor::get_user_data() { return user_data_; }

bool acceptor::parse_user_data_fields(uint8_t &message_id,
                                     size_t &expected_size) const {
  if (user_data_.size() < 1 + sizeof(uint64_t)) {
    return false;
  }
  message_id = user_data_[0];
  auto it = user_data_.begin();
  ++it; // advance past message_id
  uint64_t tmp = 0;
  try {
    detail::deserialize(it, tmp);
  } catch (...) {
    return false;
  }
  expected_size = static_cast<size_t>(tmp);
  return true;
}

acceptor::~acceptor() {}

} // namespace rdmapp
