import argparse
import socket
import sys
import select
import time
from utils import PacketHeader, compute_checksum
TIMEOUT = 0.5  # 500 milliseconds
MAX_DATA_SIZE = 1472 - 16  # Kích thước payload tối đa (1456 bytes)
def sender(receiver_ip, receiver_port, window_size):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)  # Đặt non-blocking để dùng với select.select()
    server_address = (receiver_ip, receiver_port)
    raw_data = sys.stdin.buffer.read()
    chunks = [raw_data[i:i+MAX_DATA_SIZE] for i in range(0, len(raw_data), MAX_DATA_SIZE)]
    total_packets = len(chunks)
    start_header = PacketHeader(type=0, seq_num=0, length=0)
    start_header.checksum = compute_checksum(start_header / b"")
    start_pkt = bytes(start_header / b"")
    sock.sendto(start_pkt, server_address)
    while True:
        ready = select.select([sock], [], [], 1.0)
        if ready[0]:
            ack_data, _ = sock.recvfrom(2048)
            break
    base = 1         # Sequence number nhỏ nhất chưa được ACK
    next_seq = 1     # Sequence number tiếp theo sẽ gửi đi
    while base <= total_packets:
        while next_seq < base + window_size and next_seq <= total_packets:
            chunk = chunks[next_seq - 1] # Index mảng bắt đầu từ 0
            pkt_header = PacketHeader(type=2, seq_num=next_seq, length=len(chunk))
            pkt_header.checksum = compute_checksum(pkt_header / chunk)
            pkt = bytes(pkt_header / chunk)
            sock.sendto(pkt, server_address)
            next_seq += 1
        ready = select.select([sock], [], [], TIMEOUT)
        if ready[0]: # Nhận được ACK
            while True:
                try:
                    ack_pkt, _ = sock.recvfrom(2048)
                    if len(ack_pkt) < 16:
                        continue
                    ack_header = PacketHeader(ack_pkt[:16])
                    ack_type = ack_header.type
                    ack_seq = ack_header.seq_num
                    ack_checksum_received = ack_header.checksum
                    temp_header = PacketHeader(type=ack_type, seq_num=ack_seq, length=0)
                    calc_checksum = compute_checksum(temp_header / b"")
                    if calc_checksum == ack_checksum_received and ack_type == 3:
                        if ack_seq > base:
                            base = ack_seq # Kéo cửa sổ trượt lên
                except BlockingIOError:
                    break # Hết gói trong buffer thì thoát vòng đọc
        else:
            next_seq = base
    end_header = PacketHeader(type=1, seq_num=base, length=0)
    end_header.checksum = compute_checksum(end_header / b"")
    end_pkt = bytes(end_header / b"")
    sock.sendto(end_pkt, server_address)
    start_time = time.time()
    while time.time() - start_time < 0.5:
        ready = select.select([sock], [], [], 0.5 - (time.time() - start_time))
        if ready[0]:
            try:
                ack_pkt, _ = sock.recvfrom(2048)
                if len(ack_pkt) >= 16:
                    ack_header = PacketHeader(ack_pkt[:16])
                    if ack_header.type == 3:
                        break # Nhận được ACK của END, có thể thoát ngay lập tức
            except BlockingIOError:
                pass
    sock.close()
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)
if __name__ == "__main__":
    main()