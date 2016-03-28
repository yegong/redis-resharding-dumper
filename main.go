package main

import (
	"os"
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
	"net/http"
	"github.com/cupcake/rdb"
	"github.com/cupcake/rdb/nopdecoder"
)

import _ "net/http/pprof"

var (
	masterPort int
	masterHost string
	slavePort  int
	slaveHost  string
//
	totalCount int
	lastCount  int
)

const (
	bufSize	   = 16384
	channelBuffer = 100
)

type RedisCommand struct {
	raw	  []byte
	command  []string
	reply	string
	bulkSize int64
}

func SendRedisCommand(output chan<- []byte, command...interface{}) {
	var buf = "SendRedisCommand "
	for _, line := range command {
		switch line := line.(type) {
		case string:
			buf += line
		case []byte:
			buf += string(line)
		}
		buf += " "
	}
	log.Println(buf)
	output <- []byte(fmt.Sprintf("*%d\r\n", len(command)))
	for _, line := range command {
		switch line := line.(type) {
		case string:
			output <- []byte(fmt.Sprintf("$%d\r\n", len(line)))
			output <- []byte(line)
			output <- []byte("\r\n")
		case []byte:
			output <- []byte(fmt.Sprintf("$%d\r\n", len(line)))
			output <- line
			output <- []byte("\r\n")
		}
	}
	totalCount += 1
}

func ReadRedisCommand(reader *bufio.Reader) (*RedisCommand, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Failed to read command: %v", err)
	}

	if header == "\n" || header == "\r\n" {
		// empty command
		return &RedisCommand{raw: []byte(header)}, nil
	}

	if strings.HasPrefix(header, "+") {
		return &RedisCommand{raw: []byte(header), reply: strings.TrimSpace(header[1:])}, nil
	}

	if strings.HasPrefix(header, "$") {
		bulkSize, err := strconv.ParseInt(strings.TrimSpace(header[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Unable to decode bulk size: %v", err)
		}
		return &RedisCommand{raw: []byte(header), bulkSize: bulkSize}, nil
	}

	if strings.HasPrefix(header, "*") {
		cmdSize, err := strconv.Atoi(strings.TrimSpace(header[1:]))
		if err != nil {
			return nil, fmt.Errorf("Unable to parse command length: %v", err)
		}

		result := &RedisCommand{raw: []byte(header), command: make([]string, cmdSize)}

		for i := range result.command {
			header, err = reader.ReadString('\n')
			if !strings.HasPrefix(header, "$") || err != nil {
				return nil, fmt.Errorf("Failed to read command: %v", err)
			}

			result.raw = append(result.raw, []byte(header)...)

			argSize, err := strconv.Atoi(strings.TrimSpace(header[1:]))
			if err != nil {
				return nil, fmt.Errorf("Unable to parse argument length: %v", err)
			}

			argument := make([]byte, argSize)
			_, err = io.ReadFull(reader, argument)
			if err != nil {
				return nil, fmt.Errorf("Failed to read argument: %v", err)
			}

			result.raw = append(result.raw, argument...)

			header, err = reader.ReadString('\n')
			if err != nil {
				return nil, fmt.Errorf("Failed to read argument: %v", err)
			}

			result.raw = append(result.raw, []byte(header)...)

			result.command[i] = string(argument)
		}

		return result, nil
	}

	return &RedisCommand{raw: []byte(header), command: []string{strings.TrimSpace(header)}}, nil
}

func RunMasterConnection(slavechannel chan<- []byte) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", masterHost, masterPort))
	if err != nil {
		log.Printf("Failed to connect to master: %v\n", err)
		os.Exit(2)
	}

	defer conn.Close()

	masterchannel := make(chan []byte, channelBuffer)
	defer close(masterchannel)

	go RunWriter(conn, "master", masterchannel)

	reader := bufio.NewReaderSize(conn, bufSize)

	SendRedisCommand(masterchannel, "SYNC")

	for {
		command, err := ReadRedisCommand(reader)
		if (command != nil && command.command != nil && len(command.command) > 0) {
			log.Printf("Receive from master %v\n", command.command)
		}
		if err != nil {
			log.Printf("Error while reading from master: %v\n", err)
			return
		}

		if command.reply != "" || command.command == nil && command.bulkSize == 0 {
			log.Printf("Reply %v\n", command.command)
		} else if len(command.command) == 1 && command.command[0] == "PING" {
			log.Println("Got PING from master")
			masterchannel <- []byte("\r\n")
		} else if command.bulkSize > 0 {
			log.Printf("RDB size: %d\n", command.bulkSize)
			rdb.Decode(reader, &decoder{ output: slavechannel })
			reader.Discard(8) // Skip CRC Bytes (actually, not sure)
			log.Println("RDB filtering finished, filtering commands...")
		} else {
			if len(command.command) > 1 {
				if command.command[0] == "PUBLISH" || command.command[0] == "SELECT" {
					log.Printf("Not supported by twemproxy, skip %v\n", command.command[0])
					continue
				}
			}
			slavechannel <- command.raw
		}

	}
}

// Connect to slave
func RunSlaveConnection() {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", slaveHost, slavePort))
	if err != nil {
		log.Printf("Failed to connect to slave: %v\n", err)
		os.Exit(2)
	}

	defer conn.Close()

	slavechannel := make(chan []byte, channelBuffer)
	defer close(slavechannel)

	go RunMasterConnection(slavechannel)

	go RunWriter(conn, "slave", slavechannel)

	reader := bufio.NewReaderSize(conn, bufSize)

	for {
		_, err := reader.ReadString('\n')
		if (err != nil) {
			log.Printf("Failed to read from slave: %v\n", err)
			os.Exit(2)
		}
	}
}

// Goroutine that handles writing commands to master
func RunWriter(conn net.Conn, name string, channel <-chan []byte) {
	for data := range channel {
		_, err := conn.Write(data)
		if err != nil {
			log.Printf("Failed to write data to %v: %v\n", name, err)
			os.Exit(2)
		}
	}
}

func RunStats() {
	for {
		time.Sleep(1 * time.Second)
		var currentTotalCount = totalCount
		log.Printf("QPS= %d\n", currentTotalCount - lastCount)
		lastCount = currentTotalCount
	}
}

type decoder struct {
	nopdecoder.NopDecoder
	output          chan<- []byte
	expiry          int64
}

func (p *decoder) StartRDB() {
	log.Printf("Start RDB\n")
}

func (p *decoder) Set(key, value []byte, expiry int64) {
    //SendRedisCommand(p.output, "SET", key, value)
	if (expiry > 0) {
		//SendRedisCommand(p.output, "PEXPIREAT", key, strconv.FormatInt(expiry, 10))
	}
}

func (p *decoder) StartHash(key []byte, length, expiry int64) {
	p.expiry = expiry
}

func (p *decoder) Hset(key, field, value []byte) {
	//SendRedisCommand(p.output, "HMSET", key, field, value)
}

func (p *decoder) EndHash(key []byte) {
	if (p.expiry > 0) {
		//SendRedisCommand(p.output, "PEXPIREAT", key, strconv.FormatInt(p.expiry, 10))
		p.expiry = 0
	}
}

func (p *decoder) StartSet(key []byte, cardinality, expiry int64) {
	p.expiry = expiry
}

func (p *decoder) Sadd(key, member []byte) {
	//SendRedisCommand(p.output, "SADD", key, member)
}

func (p *decoder) EndSet(key []byte) {
	if (p.expiry > 0) {
		//SendRedisCommand(p.output, "PEXPIREAT", key, strconv.FormatInt(p.expiry, 10))
		p.expiry = 0
	}
}

func (p *decoder) StartList(key []byte, length, expiry int64) {
	p.expiry = expiry
}

func (p *decoder) Rpush(key, value []byte) {
	//SendRedisCommand(p.output, "RPUSH", key, value)
}

func (p *decoder) EndList(key []byte) {
	if (p.expiry > 0) {
		//SendRedisCommand(p.output, "PEXPIREAT", key, strconv.FormatInt(p.expiry, 10))
		p.expiry = 0
	}
}

func (p *decoder) StartZSet(key []byte, cardinality, expiry int64) {
	p.expiry = expiry
}


func (p *decoder) Zadd(key []byte, score float64, member []byte) {
	//SendRedisCommand(p.output, "ZADD", key, strconv.FormatFloat(score, 'f', -1, 64), member)
}

func (p *decoder) EndZSet(key []byte) {
	if (p.expiry > 0) {
		//SendRedisCommand(p.output, "PEXPIREAT", key, strconv.FormatInt(p.expiry, 10))
		p.expiry = 0
	}
}

func (p *decoder) EndRDB() {
	log.Printf("End RDB\n")
}


func main() {
	flag.StringVar(&masterHost, "master-host", "localhost", "Master Redis host")
	flag.IntVar(&masterPort, "master-port", 6379, "Master Redis port")
	flag.StringVar(&slaveHost, "slave-host", "localhost", "Slave Redis host")
	flag.IntVar(&slavePort, "slave-port", 6380, "Slave Redis port")
	flag.Parse()

	log.Printf("Redis Resharding Proxy configured for Redis master at %s:%d\n", masterHost, masterPort)
	log.Printf("Redis Resharding Proxy configured for Redis slave at %s:%d\n", slaveHost, slavePort)
	go RunStats()
	go func() {
		log.Println(http.ListenAndServe("localhost:3003", nil))
	}()
	RunSlaveConnection()
}
