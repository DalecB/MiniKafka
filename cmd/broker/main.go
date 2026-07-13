package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"minikafka/internal/log"
	"net"
)

func handleConn(conn net.Conn, t *log.Topic, s *log.OffsetStore) {
	defer conn.Close()

	for {
		op := make([]byte, 1)
		_, err := io.ReadFull(conn, op)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed from client: ", conn.RemoteAddr().String())
			} else {
				fmt.Println("Failed to receive data: ", err)
			}
			break
		}

		switch op[0] {
		case 'P':
			keyBuf := make([]byte, 4)
			_, err := io.ReadFull(conn, keyBuf)
			if err != nil {
				return
			}

			n := binary.BigEndian.Uint32(keyBuf)

			key := make([]byte, n)
			_, err = io.ReadFull(conn, key)
			if err != nil {
				return
			}

			lenBuf := make([]byte, 4)

			_, err = io.ReadFull(conn, lenBuf)
			if err != nil {
				return
			}

			n = binary.BigEndian.Uint32(lenBuf)

			payload := make([]byte, n)
			_, err = io.ReadFull(conn, payload)
			if err != nil {
				return
			}

			partition, offset, err := t.Append(key, payload)
			if err != nil {
				fmt.Println("Failed to append log: ", err)
				return
			}

			resp := make([]byte, 10)
			resp[0] = 0x00
			resp[1] = byte(partition)
			binary.BigEndian.PutUint64(resp[2:], uint64(offset))

			_, err = conn.Write(resp)
			if err != nil {
				fmt.Println("Failed to write: ", err)
				return
			}

		case 'F':
			partBuf := make([]byte, 1)
			_, err := io.ReadFull(conn, partBuf)
			if err != nil {
				return
			}

			partition := int(partBuf[0])

			offBuf := make([]byte, 8)

			_, err = io.ReadFull(conn, offBuf)
			if err != nil {
				return
			}

			offset := binary.BigEndian.Uint64(offBuf)

			status := 0x00
			payload, err := t.Read(partition, int64(offset))
			if err != nil {
				fmt.Println("Failed to read data: ", err)
				status = 0x01
			}

			resp := make([]byte, 5+len(payload))
			resp[0] = byte(status)
			binary.BigEndian.PutUint32(resp[1:5], uint32(len(payload)))
			copy(resp[5:], payload)

			_, err = conn.Write(resp)
			if err != nil {
				fmt.Println("Failed to write: ", err)
			}

		case 'C':
			glBuf := make([]byte, 4)
			_, err := io.ReadFull(conn, glBuf)
			if err != nil {
				return
			}

			gl := binary.BigEndian.Uint32(glBuf)
			group := make([]byte, gl)
			_, err = io.ReadFull(conn, group)
			if err != nil {
				return
			}

			partBuf := make([]byte, 1)
			_, err = io.ReadFull(conn, partBuf)
			if err != nil {
				return
			}
			partition := int(partBuf[0])

			offBuf := make([]byte, 8)
			_, err = io.ReadFull(conn, offBuf)
			if err != nil {
				return
			}
			offset := binary.BigEndian.Uint64(offBuf)

			resp := make([]byte, 1)
			err = s.Commit(string(group), partition, int64(offset))
			if err != nil {
				resp[0] = 0x01 // ERR
			} else {
				resp[0] = 0x00 // OK
			}

			_, err = conn.Write(resp)
			if err != nil {
				fmt.Println("failed to write commit response: ", err)
			}

		case 'O':
			glBuf := make([]byte, 4)
			_, err := io.ReadFull(conn, glBuf)
			if err != nil {
				return
			}

			gl := binary.BigEndian.Uint32(glBuf)
			group := make([]byte, gl)
			_, err = io.ReadFull(conn, group)
			if err != nil {
				return
			}

			partBuf := make([]byte, 1)
			_, err = io.ReadFull(conn, partBuf)
			if err != nil {
				return
			}
			partition := int(partBuf[0])

			offset := s.GetCommitted(string(group), partition)

			resp := make([]byte, 9)
			resp[0] = 0x00
			binary.BigEndian.PutUint64(resp[1:], uint64(offset))

			_, err = conn.Write(resp)
			if err != nil {
				fmt.Println("failed to write committed response: ", err)
			}

		default:
			return
		}
	}

}

func main() {
	fsyncN := flag.Int("fsync", 0, "fsync: 0=안함, 1=매번, N=N건마다")
	flag.Parse()

	ls, err := net.Listen("tcp", ":9092")
	if err != nil {
		fmt.Println("Failed to listen: ", err)
		return
	}
	defer ls.Close()

	t, err := log.NewTopic("data", *fsyncN)
	if err != nil {
		fmt.Println("Failed to generate log: ", err)
		return
	}

	store, err := log.NewOffsetStore("data/__offsets")
	if err != nil {
		fmt.Println("Failed to generate offset store: ", err)
		return
	}

	for {
		conn, err := ls.Accept()
		if err != nil {
			fmt.Println("Failed to accept: ", err)
			continue
		}

		go handleConn(conn, t, store)
	}
}
