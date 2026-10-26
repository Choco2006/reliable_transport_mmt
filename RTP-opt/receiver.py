import socket
import sys
from utils import PacketHeader, compute_checksum
def make_ack(seq_num):
    header = PacketHeader(type=3, seq_num=seq_num, length=0)
    header.checksum = compute_checksum(header / b"")
    return bytes(header / b"")
def main():
    if len(sys.argv) != 4:
        sys.exit(1)
    receiver_ip = sys.argv[1]
    receiver_port = int(sys.argv[2])
    window_size = int(sys.argv[3])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((receiver_ip, receiver_port))
    expected_seq = 1
    buffer = {}
    connected = False
    while True:
        try:
            pkt, addr = sock.recvfrom(2048)
            if len(pkt) < 16:
                continue
            header_bytes = pkt[:16]
            data = pkt[16:]
            try:
                header = PacketHeader(header_bytes)
            except Exception:
                continue 
            msg_type = header.type
            seq_num = header.seq_num
            length = header.length
            pkt_checksum = header.checksum
            temp_header = PacketHeader(type=msg_type, seq_num=seq_num, length=length)
            calc_checksum = compute_checksum(temp_header / data)
            if calc_checksum != pkt_checksum:
                continue 
            if msg_type == 0: 
                if not connected:
                    connected = True
                    expected_seq = 1
                    buffer.clear()
                sock.sendto(make_ack(1), addr)
            elif msg_type == 2: 
                if not connected:
                    continue
                sock.sendto(make_ack(seq_num), addr)
                if seq_num == expected_seq:
                    sys.stdout.buffer.write(data)
                    sys.stdout.flush()
                    expected_seq += 1
                    while expected_seq in buffer:
                        sys.stdout.buffer.write(buffer[expected_seq])
                        sys.stdout.flush()
                        del buffer[expected_seq]
                        expected_seq += 1
                elif expected_seq < seq_num < expected_seq + window_size:
                    buffer[seq_num] = data
            elif msg_type == 1: # Gói END
                if not connected:
                    continue
                sock.sendto(make_ack(seq_num), addr)
                sys.stdout.flush()
                sys.exit(0)
        except KeyboardInterrupt:
            sys.exit(0)
if __name__ == "__main__":
    main()