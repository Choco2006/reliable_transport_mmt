import socket
import sys
from utils import PacketHeader, compute_checksum

def generate_ack_packet(seq):
    """Tạo gói tin ACK phản hồi nhanh"""
    ack_hdr = PacketHeader(type=3, seq_num=seq, length=0)
    ack_hdr.checksum = compute_checksum(ack_hdr / b"")
    return bytes(ack_hdr / b"")

def is_corrupted(pkt_type, seq, length, data, received_checksum):
    """Kiểm tra tính toàn vẹn của dữ liệu"""
    dummy_hdr = PacketHeader(type=pkt_type, seq_num=seq, length=length)
    return compute_checksum(dummy_hdr / data) != received_checksum

def flush_data(data):
    """Ghi dữ liệu ra luồng tiêu chuẩn ngay lập tức"""
    sys.stdout.buffer.write(data)
    sys.stdout.flush()

def main():
    if len(sys.argv) != 4:
        sys.exit(1)

    host_ip = sys.argv[1]
    port = int(sys.argv[2])
    win_size = int(sys.argv[3])

    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind((host_ip, port))

    next_needed = 1
    out_of_order_buf = {}
    is_active = False

    while True:
        try:
            raw_pkt, sender_addr = udp_socket.recvfrom(2048)
            if len(raw_pkt) < 16:
                continue

            try:
                hdr = PacketHeader(raw_pkt[:16])
            except Exception:
                continue 

            payload = raw_pkt[16:]
            
            # Kiểm tra lỗi mạng
            if is_corrupted(hdr.type, hdr.seq_num, hdr.length, payload, hdr.checksum):
                continue 

            # Phân loại và xử lý
            if hdr.type == 0: # START
                if not is_active:
                    is_active = True
                    next_needed = 1
                    out_of_order_buf.clear()
                udp_socket.sendto(generate_ack_packet(1), sender_addr)
            
            elif hdr.type == 2 and is_active: # DATA
                # Gửi Individual ACK lập tức
                udp_socket.sendto(generate_ack_packet(hdr.seq_num), sender_addr)

                if hdr.seq_num == next_needed:
                    flush_data(payload)
                    next_needed += 1

                    # Kéo dữ liệu từ buffer ra nếu đã nhận từ trước
                    while next_needed in out_of_order_buf:
                        flush_data(out_of_order_buf.pop(next_needed))
                        next_needed += 1
                
                elif next_needed < hdr.seq_num < next_needed + win_size:
                    out_of_order_buf[hdr.seq_num] = payload

            elif hdr.type == 1 and is_active: # END
                udp_socket.sendto(generate_ack_packet(hdr.seq_num), sender_addr)
                sys.stdout.flush()
                sys.exit(0)

        except KeyboardInterrupt:
            sys.exit(0)

if __name__ == "__main__":
    main()