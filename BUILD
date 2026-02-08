# BUILD file for rdmapp (RDMA++ C++ library).
#
# Requires (system):
#   - rdma-core: libibverbs and libmlx5 (e.g. apt install libibverbs-dev rdma-core)
#   - C++20 toolchain (gcc 10+ or clang 10+)
#
# Explore:   bazel build //third_party/rdmapp:rdmapp --cxxopt=-std=c++20
# As dependency: deps = ["//third_party/rdmapp:rdmapp"]
#
# See third_party/rdmapp/README.bazel for integration notes.

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "rdmapp",
    srcs = [
        "src/cq.cc",
        "src/cq_poller.cc",
        "src/device.cc",
        "src/executor.cc",
        "src/mr.cc",
        "src/pd.cc",
        "src/qp.cc",
        "src/srq.cc",
    ],
    hdrs = [
        "include/rdmapp/cq.h",
        "include/rdmapp/cq_poller.h",
        "include/rdmapp/device.h",
        "include/rdmapp/error.h",
        "include/rdmapp/executor.h",
        "include/rdmapp/mr.h",
        "include/rdmapp/pd.h",
        "include/rdmapp/qp.h",
        "include/rdmapp/rdmapp.h",
        "include/rdmapp/srq.h",
        "include/rdmapp/task.h",
        "include/rdmapp/detail/blocking_queue.h",
        "include/rdmapp/detail/debug.h",
        "include/rdmapp/detail/noncopyable.h",
        "include/rdmapp/detail/serdes.h",
    ],
    strip_include_prefix = "include",
    copts = [
        "-std=c++20",
        "-fno-rtti",
        "-Wall",
        "-Wextra",
    ],
    linkopts = [
        "-libverbs",
        "-lmlx5",
        "-lpthread",
    ],
)
