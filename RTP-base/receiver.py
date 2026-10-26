import socket
import sys
import struct
import zlib
HEADER_FORMAT = '!IIII'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
def compute_checksum(msg_type, seq_num, length, data):
    dummy_header = struct.pack(HEADER_FORMAT, msg_type, seq_num, length, 0)
    packet_data = dummy_header + data
    return zlib.crc32(packet_data) & 0xffffffff
def make_ack(seq_num):
    msg_type = 3
    length = 0
    data = b''
    checksum = compute_checksum(msg_type, seq_num, length, data)
    return struct.pack(HEADER_FORMAT, msg_type, seq_num, length, checksum) + data
def main():
    if len(sys.argv) != 4:
        print("Usage: python receiver.py [receiver_ip] [receiver_port] [window_size]")
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
            if len(pkt) < HEADER_SIZE:
                continue
            header = pkt[:HEADER_SIZE]
            data = pkt[HEADER_SIZE:]
            msg_type, seq_num, length, pkt_checksum = struct.unpack(HEADER_FORMAT, header)
            calc_checksum = compute_checksum(msg_type, seq_num, length, data)
            if calc_checksum != pkt_checksum:
                continue 
            if msg_type == 0:
                if not connected:
                    connected = True
                    expected_seq = 1
                    buffer.clear()
                sock.sendto(make_ack(1), addr)
                continue
            elif msg_type == 2:
                if not connected:
                    continue
                if seq_num == expected_seq:
                    sys.stdout.buffer.write(data)
                    expected_seq += 1
                    while expected_seq in buffer:
                        sys.stdout.buffer.write(buffer[expected_seq])
                        del buffer[expected_seq]
                        expected_seq += 1
                elif expected_seq < seq_num < expected_seq + window_size:
                    buffer[seq_num] = data
                sock.sendto(make_ack(expected_seq), addr)
            elif msg_type == 1:
                if not connected:
                    continue
                sock.sendto(make_ack(expected_seq), addr)
                sys.stdout.flush()
                sys.exit(0)
        except KeyboardInterrupt:
            print("\nReceiver shut down.")
            sys.exit(0)
if __name__ == "__main__":
    main()