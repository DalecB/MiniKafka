package client

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

type Client struct {
	conn net.Conn
}

func NewClient(addr string) (*Client, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: c}, nil
}

func (c *Client) Produce(key []byte, payload []byte) (int, int64, error) {
	msg := []byte{'P'}

	keyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))
	msg = append(msg, keyLen...)
	msg = append(msg, key...)

	payLen := make([]byte, 4)
	binary.BigEndian.PutUint32(payLen, uint32(len(payload)))
	msg = append(msg, payLen...)
	msg = append(msg, payload...)

	_, err := c.conn.Write(msg)
	if err != nil {
		return 0, 0, err
	}

	resp := make([]byte, 10)
	_, err = io.ReadFull(c.conn, resp)
	if err != nil {
		return 0, 0, err
	}

	if resp[0] != 0x00 {
		return 0, 0, errors.New("produce failed")
	}

	partition := int(resp[1])
	offset := int64(binary.BigEndian.Uint64(resp[2:]))
	return partition, offset, nil
}

func (c *Client) Fetch(partition int, offset int64) ([]byte, error) {
	buf := make([]byte, 10)
	buf[0] = 'F'
	buf[1] = byte(partition)
	binary.BigEndian.PutUint64(buf[2:], uint64(offset))

	_, err := c.conn.Write(buf)
	if err != nil {
		return nil, err
	}

	head := make([]byte, 5)
	_, err = io.ReadFull(c.conn, head)
	if err != nil {
		return nil, err
	}

	if head[0] != 0x00 {
		return nil, errors.New("fetch failed")
	}

	n := binary.BigEndian.Uint32(head[1:5])

	payload := make([]byte, n)
	_, err = io.ReadFull(c.conn, payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func (c *Client) Commit(group string, partition int, offset int64) error {
	msg := []byte{'C'}

	glBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(glBuf, uint32(len(group)))
	msg = append(msg, glBuf...)
	msg = append(msg, group...)
	msg = append(msg, byte(partition))

	offBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(offBuf, uint64(offset))
	msg = append(msg, offBuf...)

	_, err := c.conn.Write(msg)
	if err != nil {
		return err
	}

	resp := make([]byte, 1)
	_, err = io.ReadFull(c.conn, resp)
	if err != nil {
		return err
	}

	if resp[0] != 0x00 {
		return errors.New("commit failed")
	}
	return nil
}

func (c *Client) GetCommitted(group string, partition int) (int64, error) {
	msg := []byte{'O'}

	glBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(glBuf, uint32(len(group)))
	msg = append(msg, glBuf...)
	msg = append(msg, group...)
	msg = append(msg, byte(partition))

	_, err := c.conn.Write(msg)
	if err != nil {
		return 0, err
	}

	resp := make([]byte, 9)
	_, err = io.ReadFull(c.conn, resp)
	if err != nil {
		return 0, err
	}

	if resp[0] != 0x00 {
		return 0, errors.New("get committed failed")
	}

	offset := int64(binary.BigEndian.Uint64(resp[1:]))
	return offset, nil
}
