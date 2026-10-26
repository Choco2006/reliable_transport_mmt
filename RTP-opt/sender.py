import argparse
import socket
import sys
import select
import time
from utils import PacketHeader, compute_checksum
TIMEOUT = 0.5
MAX_DATA_SIZE = 1456 
def sender(receiver_ip, receiver_port, window_size):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    server_address = (receiver_ip, receiver_port)
    raw_data = sys.stdin.buffer.read()
    chunks = [raw_data[i:i+MAX_DATA_SIZE] for i in range(0, len(raw_data), MAX_DATA_SIZE)]
    total_packets = len(chunks)
    start_header = PacketHeader(type=0, seq_num=0, length=0)
    start_header.checksum = compute_checksum(start_header / b"")
    sock.sendto(bytes(start_header / b""), server_address)
    while True:
        ready = select.select([sock], [], [], 1.0)
        if ready[0]:
            try:
                sock.recvfrom(2048)
                break
            except BlockingIOError:
                pass
    base = 1
    next_seq = 1
    acked_packets = set() # Chứa các gói ĐÃ CÓ ACK trong cửa sổ
    timer_start_time = time.time()
    while base <= total_packets:
        while next_seq < base + window_size and next_seq <= total_packets:
            chunk = chunks[next_seq - 1]
            pkt_header = PacketHeader(type=2, seq_num=next_seq, length=len(chunk))
            pkt_header.checksum = compute_checksum(pkt_header / chunk)
            sock.sendto(bytes(pkt_header / chunk), server_address)
            next_seq += 1
        time_left = TIMEOUT - (time.time() - timer_start_time)
        if time_left <= 0:
            for seq in range(base, next_seq):
                if seq not in acked_packets:
                    chunk = chunks[seq - 1]
                    pkt_header = PacketHeader(type=2, seq_num=seq, length=len(chunk))
                    pkt_header.checksum = compute_checksum(pkt_header / chunk)
                    sock.sendto(bytes(pkt_header / chunk), server_address)
            timer_start_time = time.time() # Reset timer
            continue
        ready = select.select([sock], [], [], time_left)
        if ready[0]:
            advanced = False
            while True:
                try:
                    ack_pkt, _ = sock.recvfrom(2048)
                    if len(ack_pkt) < 16:
                        continue
                    try:
                        ack_header = PacketHeader(ack_pkt[:16])
                    except Exception:
                        continue
                    temp_header = PacketHeader(type=ack_header.type, seq_num=ack_header.seq_num, length=0)
                    calc_chk = compute_checksum(temp_header / b"")
                    if calc_chk == ack_header.checksum and ack_header.type == 3:
                        ack_seq = ack_header.seq_num
                        if base <= ack_seq < base + window_size:
                            acked_packets.add(ack_seq)
                            while base in acked_packets:
                                acked_packets.remove(base)
                                base += 1
                                advanced = True
                except BlockingIOError:
                    break
            if advanced:
                timer_start_time = time.time()
    end_header = PacketHeader(type=1, seq_num=base, length=0)
    end_header.checksum = compute_checksum(end_header / b"")
    sock.sendto(bytes(end_header / b""), server_address)
    start_time = time.time()
    while time.time() - start_time < 0.5:
        ready = select.select([sock], [], [], 0.5 - (time.time() - start_time))
        if ready[0]:
            try:
                ack_pkt, _ = sock.recvfrom(2048)
                if len(ack_pkt) >= 16:
                    ack_header = PacketHeader(ack_pkt[:16])
                    if ack_header.type == 3:
                        break
            except BlockingIOError:
                pass
    sock.close()
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)
if __name__ == "__main__":
    main()