package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/marksamman/bencode"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	//"go.etcd.io/etcd/pkg/fileutil"
)

const (
	directory = "torrents"
)

type tfile struct {
	name   string
	length int64
}

type TFile struct {
	Name   string `json:"name"`
	Length int64  `json:"length"`
}

func (t *tfile) String() string {
	return fmt.Sprintf("name: %s\n, size: %d\n", t.name, t.length)
}

type torrent struct {
	infohashHex string
	name        string
	length      int64
	pieceLength int64
	files       []*tfile
	TFiles      []*TFile
}

func (t *torrent) String() string {
	jsonBytes, _ := json.Marshal(t.TFiles)
	return fmt.Sprintf(
		"link: %s\nname: %s\nsize: %d\nfileNum: %d\n, files: %s\n",
		fmt.Sprintf("magnet:?xt=urn:btih:%s", t.infohashHex),
		t.name,
		t.length,
		len(t.files),
		string(jsonBytes),
	)
}

func parseTorrent(meta []byte, infohashHex string) (*torrent, error) {
	dict, err := bencode.Decode(bytes.NewBuffer(meta))
	if err != nil {
		return nil, err
	}

	t := &torrent{infohashHex: infohashHex}
	if name, ok := dict["name.utf-8"].(string); ok {
		t.name = name
	} else if name, ok := dict["name"].(string); ok {
		t.name = name
	}
	if length, ok := dict["length"].(int64); ok {
		t.length = length
	}

	if pieceLength, ok := dict["piece length"].(int64); ok {
		t.pieceLength = pieceLength
	}

	var totalSize int64
	var extractFiles = func(file map[string]interface{}) {
		var filename string
		var filelength int64
		if inter, ok := file["path.utf-8"].([]interface{}); ok {
			name := make([]string, len(inter))
			for i, v := range inter {
				name[i] = fmt.Sprint(v)
			}
			filename = strings.Join(name, "/")
		} else if inter, ok := file["path"].([]interface{}); ok {
			name := make([]string, len(inter))
			for i, v := range inter {
				name[i] = fmt.Sprint(v)
			}
			filename = strings.Join(name, "/")
		}
		if length, ok := file["length"].(int64); ok {
			filelength = length
			totalSize += filelength
		}
		t.files = append(t.files, &tfile{name: filename, length: filelength})
		t.TFiles = append(t.TFiles, &TFile{Name: filename, Length: filelength})
	}

	if files, ok := dict["files"].([]interface{}); ok {
		for _, file := range files {
			if f, ok := file.(map[string]interface{}); ok {
				extractFiles(f)
			}
		}
	}

	if t.length == 0 {
		t.length = totalSize
	}
	if len(t.files) == 0 {
		t.files = append(t.files, &tfile{name: t.name, length: t.length})
		t.TFiles = append(t.TFiles, &TFile{Name: t.name, Length: t.length})
	}

	return t, nil
}

type torsniff struct {
	laddr      string
	maxFriends int
	maxPeers   int
	secret     string
	timeout    time.Duration
	blacklist  *blackList
	dir        string
	db         *sql.DB
}

func (t *torsniff) run() error {
	tokens := make(chan struct{}, t.maxPeers)

	dht, err := newDHT(t.laddr, t.maxFriends)
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", "torsniff:taobao1234@/torrent?charset=utf8mb4")
	if err != nil {
		return err
	}
	t.db = db

	dht.run()

	log.Println("running, it may take a few minutes...")

	for {
		select {
		case <-dht.announcements.wait():
			for {
				if ac := dht.announcements.get(); ac != nil {
					tokens <- struct{}{}
					go t.work(ac, tokens)
					continue
				}
				break
			}
		case <-dht.die:
			return dht.errDie
		}
	}

	return nil
}

func (t *torsniff) work(ac *announcement, tokens chan struct{}) {
	defer func() {
		<-tokens
	}()

	peerAddr := ac.peer.String()
	if t.blacklist.has(peerAddr) {
		return
	}

	wire := newMetaWire(string(ac.infohash), peerAddr, t.timeout)
	defer wire.free()

	meta, err := wire.fetch()
	if err != nil {
		t.blacklist.add(peerAddr)
		return
	}

	torrent, err := parseTorrent(meta, ac.infohashHex)

	if err := t.saveTorrent(ac.infohashHex, meta, torrent); err != nil {
		return
	}

	if err != nil {
		return
	}

	log.Println(torrent)
}

func (t *torsniff) isTorrentExist(infohashHex string, torrent *torrent) bool {
	name, _ := t.torrentPath(infohashHex, torrent)
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}

func (t *torsniff) saveTorrent(infohashHex string, data []byte, torrent *torrent) error {

	rs := t.db.QueryRow("select id from info where hash=?", infohashHex)

	var count int64
	err := rs.Scan(&count)
	switch err {
	case sql.ErrNoRows: // 没有数据说明可以插入直接跳过
		break
	case nil:
		// 查得到说明存在重复的 hash
		fmt.Printf("duplicate hash: %s count: %d \n", infohashHex, count)
		return nil
	default:
		fmt.Printf("unknown error %v %d \n", err, count)
		return nil
	}

	jsonBytes, err := json.Marshal(torrent.TFiles)
	//fmt.Printf("%v %v", torrent.TFiles, jsonBytes)
	stmt, err := t.db.Prepare("insert into info (hash, name, piece_length, files_number, total_length, files) values (?, ?, ?, ?, ?, ?)")
	_, err = stmt.Exec(infohashHex, torrent.name, torrent.pieceLength, len(torrent.files), torrent.length, string(jsonBytes))

	if err != nil {
		fmt.Printf("insert sql error, %v \n", err)
		return err
	}

	return nil
}

func (t *torsniff) torrentPath(infohashHex string, torrent *torrent) (name string, dir string) {
	dir = t.dir
	name = path.Join(dir, torrent.name+".torrent")
	return
}

func main() {
	log.SetFlags(0)

	var addr string
	var port uint16
	var peers int
	var timeout time.Duration
	var dir string
	var verbose bool
	var friends int

	home, err := homedir.Dir()
	userHome := path.Join(home, directory)

	root := &cobra.Command{
		Use:          "torsniff",
		Short:        "torsniff - A sniffer that sniffs torrents from BitTorrent network.",
		SilenceUsage: true,
	}
	root.RunE = func(cmd *cobra.Command, args []string) error {
		if dir == userHome && err != nil {
			return err
		}

		absDir, err := filepath.Abs(dir)
		if err != nil {
			return err
		}

		log.SetOutput(ioutil.Discard)
		if verbose {
			log.SetOutput(os.Stdout)
		}

		p := &torsniff{
			laddr:      fmt.Sprintf("%s:%d", addr, port),
			timeout:    timeout,
			maxFriends: friends,
			maxPeers:   peers,
			secret:     string(randBytes(20)),
			dir:        absDir,
			blacklist:  newBlackList(5*time.Minute, 50000),
		}
		return p.run()
	}

	root.Flags().StringVarP(&addr, "addr", "a", "0.0.0.0", "listen on given address")
	root.Flags().Uint16VarP(&port, "port", "p", 6882, "listen on given port")
	root.Flags().IntVarP(&friends, "friends", "f", 5000, "max fiends to make with per second")
	root.Flags().IntVarP(&peers, "peers", "e", 5000, "max peers to connect to download torrents")
	root.Flags().DurationVarP(&timeout, "timeout", "t", 10*time.Second, "max time allowed for downloading torrents")
	root.Flags().StringVarP(&dir, "dir", "d", userHome, "the directory to store the torrents")
	root.Flags().BoolVarP(&verbose, "verbose", "v", true, "run in verbose mode")

	if err := root.Execute(); err != nil {
		fmt.Errorf("could not start: %s\n", err)
	}
}
