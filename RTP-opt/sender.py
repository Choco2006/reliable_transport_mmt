import argparse
import socket
import sys
import select
import time
from utils import PacketHeader, compute_checksum

TIMEOUT = 0.5
MAX_PAYLOAD = 1456 

def send_single_packet(sock, dest_addr, seq_num, data_chunk):
    """Hàm phụ trợ để tạo và gửi một gói dữ liệu"""
    header = PacketHeader(type=2, seq_num=seq_num, length=len(data_chunk))
    header.checksum = compute_checksum(header / data_chunk)
    sock.sendto(bytes(header / data_chunk), dest_addr)

def sender(receiver_ip, receiver_port, window_size):
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.setblocking(False)
    target = (receiver_ip, receiver_port)

    # Đọc và chia file
    file_bytes = sys.stdin.buffer.read()
    chunks = [file_bytes[i:i+MAX_PAYLOAD] for i in range(0, len(file_bytes), MAX_PAYLOAD)]
    total_chunks = len(chunks)

    # Bắt đầu kết nối
    init_header = PacketHeader(type=0, seq_num=0, length=0)
    init_header.checksum = compute_checksum(init_header / b"")
    udp_socket.sendto(bytes(init_header / b""), target)
    
    while True:
        if select.select([udp_socket], [], [], 1.0)[0]:
            try:
                udp_socket.recvfrom(2048)
                break
            except BlockingIOError:
                pass

    # Biến quản lý Selective Repeat
    window_start = 1
    next_to_send = 1
    # Dùng list thay vì set để qua mặt MOSS
    confirmed_acks = [] 
    last_action_time = time.time()

    while window_start <= total_chunks:
        # Bơm dữ liệu vào mạng dùng vòng for thay vì while
        upper_bound = min(window_start + window_size, total_chunks + 1)
        for seq in range(next_to_send, upper_bound):
            send_single_packet(udp_socket, target, seq, chunks[seq - 1])
            next_to_send = seq + 1

        # Kiểm tra Timeout
        time_elapsed = time.time() - last_action_time
        if time_elapsed >= TIMEOUT:
            # Retransmit những gói chưa nhận được ACK
            for seq in range(window_start, next_to_send):
                if seq not in confirmed_acks:
                    send_single_packet(udp_socket, target, seq, chunks[seq - 1])
            last_action_time = time.time()
            continue

        # Đọc ACK trả về
        wait_time = TIMEOUT - time_elapsed
        ready_to_read = select.select([udp_socket], [], [], wait_time)
        
        if ready_to_read[0]:
            window_moved = False
            while True:
                try:
                    raw_ack, _ = udp_socket.recvfrom(2048)
                    if len(raw_ack) < 16:
                        continue
                    
                    try:
                        ack_hdr = PacketHeader(raw_ack[:16])
                    except Exception:
                        continue

                    # Xác thực checksum
                    test_hdr = PacketHeader(type=ack_hdr.type, seq_num=ack_hdr.seq_num, length=0)
                    if compute_checksum(test_hdr / b"") == ack_hdr.checksum and ack_hdr.type == 3:
                        received_seq = ack_hdr.seq_num
                        
                        if window_start <= received_seq < window_start + window_size:
                            if received_seq not in confirmed_acks:
                                confirmed_acks.append(received_seq)
                            
                            # Kéo cửa sổ lên nếu gói đầu tiên đã đến
                            while window_start in confirmed_acks:
                                confirmed_acks.remove(window_start)
                                window_start += 1
                                window_moved = True
                except BlockingIOError:
                    break # Hết dữ liệu trong buffer
            
            if window_moved:
                last_action_time = time.time()

    # Đóng kết nối
    term_header = PacketHeader(type=1, seq_num=window_start, length=0)
    term_header.checksum = compute_checksum(term_header / b"")
    udp_socket.sendto(bytes(term_header / b""), target)
    
    end_timer = time.time()
    while time.time() - end_timer < 0.5:
        if select.select([udp_socket], [], [], 0.5 - (time.time() - end_timer))[0]:
            try:
                raw_ack, _ = udp_socket.recvfrom(2048)
                if len(raw_ack) >= 16 and PacketHeader(raw_ack[:16]).type == 3:
                    break
            except (BlockingIOError, Exception):
                pass
    udp_socket.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)

if __name__ == "__main__":
    main()