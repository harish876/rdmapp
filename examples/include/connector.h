#pragma once

#include "socket/event_loop.h"
#include <memory>
#include <string>
#include <vector>

#include <rdmapp/cq.h>
#include <rdmapp/pd.h>
#include <rdmapp/qp.h>
#include <rdmapp/task.h>

#include "rdmapp/detail/noncopyable.h"

namespace rdmapp {

/**
 * @brief This class is used to actively connect to a remote endpoint and
 * establish a Queue Pair.
 *
 */
class connector : public noncopyable {
  std::shared_ptr<pd> pd_;
  std::shared_ptr<cq> recv_cq_;
  std::shared_ptr<cq> send_cq_;
  std::shared_ptr<srq> srq_;
  std::shared_ptr<socket::event_loop> loop_;
  std::string hostname_;
  uint16_t port_;
  enum ibv_qp_type qp_type_;
  // Optional user data to send during QP handshake (copied into the serialized
  // qp before send). Use set_user_data_from_string() to populate.
  std::vector<uint8_t> user_data_;

public:
  /**
   * @brief Construct a new connector object.
   *
   * @param loop The event loop to use.
   * @param hostname The hostname to connect to.
   * @param port The port to connect to.
   * @param recv_cq The recv completion queue to use for new Queue Pairs.
   * @param send_cq The send completion queue to use for new Queue Pairs.
   * @param srq (Optional) The shared receive queue to use for new Queue Pairs.
   */
  connector(std::shared_ptr<socket::event_loop> loop,
            std::string const &hostname, uint16_t port, std::shared_ptr<pd> pd,
            std::shared_ptr<cq> recv_cq, std::shared_ptr<cq> send_cq,
            std::shared_ptr<srq> srq = nullptr,
            enum ibv_qp_type qp_type = IBV_QPT_RC);

  /**
   * @brief Construct a new connector object.
   *
   * @param loop The event loop to use.
   * @param hostname The hostname to connect to.
   * @param port The port to connect to.
   * @param recv_cq The send/recv completion queue to use for new Queue Pairs.
   * @param srq (Optional) The shared receive queue to use for new Queue Pairs.
   */
  connector(std::shared_ptr<socket::event_loop> loop,
            std::string const &hostname, uint16_t port, std::shared_ptr<pd> pd,
            std::shared_ptr<cq> cq, std::shared_ptr<srq> srq = nullptr,
            enum ibv_qp_type qp_type = IBV_QPT_RC);

  /**
   * @brief This function is used to connect to a remote endpoint and establish
   * a Queue Pair.
   *
   * @return task<std::shared_ptr<qp>>
   */
  task<std::shared_ptr<qp>> connect();

  /**
   * @brief Set optional user data (as a string) that will be sent during the
   * QP handshake when calling connect(). The string's bytes are copied.
   */
  void set_user_data_from_string(const std::string &s);
  /**
   * @brief Convenience: set structured user_data containing an 8-bit
   * message_id followed by a 64-bit expected_size (network byte order).
   */
  void set_user_data_fields(uint8_t message_id, size_t expected_size);
};

} // namespace rdmapp