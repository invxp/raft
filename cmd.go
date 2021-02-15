package raft

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"strconv"
	"time"
)

type cmdServer struct {
	*http.Server
	srv      *Server
	password string
}

func (s *cmdServer) auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Query("auth") == s.password {
			c.Next()
		} else {
			_ = c.AbortWithError(http.StatusNetworkAuthenticationRequired, fmt.Errorf("need authentication"))
		}
	}
}

func (s *cmdServer) notFound(c *gin.Context) {
	c.String(http.StatusOK, "-command list: \n"+
		"---info: show information\n"+
		"---commit: commit a data(data=xxx)\n"+
		"---logs: get logs(n=x)\n"+
		"---save: save data\n",
	)
}

func (s *cmdServer) info(c *gin.Context) {
	var str string

	s.srv.mu.RLock()

	str = fmt.Sprintf("%s-server runtime: %v\n", str, time.Since(s.srv.runTime))
	str = fmt.Sprintf("%s-running: %t\n", str, !s.srv.shutdown)
	str = fmt.Sprintf("%s-server address: %s\n", str, s.srv.address)
	str = fmt.Sprintf("%s-server rpc timeout(ms): %d\n", str, s.srv.rpcMsgTimeoutMs)
	str = fmt.Sprintf("%s-server heartbeat(ms): %d\n", str, s.srv.heartbeatMs)
	str = fmt.Sprintf("%s-server election interval(ms): %d - %d\n", str, s.srv.electionTimeoutMinMs, s.srv.electionTimeoutMaxMs)
	str = fmt.Sprintf("%s-server auto redirect message: %t\n", str, s.srv.autoRedirectMessage)
	str = fmt.Sprintf("%s-server show log: %t\n", str, s.srv.showLog)

	str = fmt.Sprintf("%s-clients: \n", str)
	for k, v := range s.srv.nodeClients {
		str = fmt.Sprintf("%s--%s\n---isClose: %t\n---isConnected: %t\n", str, k, v.isClose, v.isConnected)
	}
	s.srv.mu.RUnlock()

	s.srv.raft.mu.RLock()
	str = fmt.Sprintf("%s-rpcClient state: %v\n", str, s.srv.raft.currentState)
	str = fmt.Sprintf("%s-rpcClient role time: %v\n", str, time.Since(s.srv.raft.roleTime))
	str = fmt.Sprintf("%s-rpcClient log count: %d\n", str, len(s.srv.raft.log))
	str = fmt.Sprintf("%s-rpcClient vote for: %s\n", str, s.srv.raft.votedFor)
	str = fmt.Sprintf("%s-rpcClient term: %d\n", str, s.srv.raft.currentTerm)
	str = fmt.Sprintf("%s-rpcClient commit index: %d\n", str, s.srv.raft.commitIndex)
	str = fmt.Sprintf("%s-rpcClient last applied: %d\n", str, s.srv.raft.lastApplied)
	str = fmt.Sprintf("%s-rpcClient match index: %d\n", str, s.srv.raft.matchIndex[s.srv.raft.currentAddress])
	str = fmt.Sprintf("%s-rpcClient next index: %d\n", str, s.srv.raft.nextIndex[s.srv.raft.currentAddress])
	s.srv.raft.mu.RUnlock()

	c.String(http.StatusOK, str)
}

func (s *cmdServer) commit(c *gin.Context) {
	if data := c.Query("data"); data != "" {
		c.JSONP(http.StatusOK, s.srv.Commit(data))
	} else {
		c.String(http.StatusBadRequest, "no data")
	}
}

func (s *cmdServer) logs(c *gin.Context) {
	num, _ := strconv.Atoi(c.Query("n"))
	c.JSONP(http.StatusOK, s.srv.Logs(num))
}

func (s *cmdServer) save(c *gin.Context) {
	s.srv.raft.saveToStorage()
	s.srv.raft.saveNodes()
	c.String(http.StatusOK, "")
}

func (s *cmdServer) serv(address, auth string) {
	s.password = auth

	gin.SetMode("release")

	router := gin.Default()

	router.Use(s.auth())

	router.NoRoute(s.notFound)

	router.GET("/info", s.info)
	router.GET("/commit", s.commit)
	router.GET("/logs", s.logs)
	router.GET("/save", s.save)

	s.Server = &http.Server{
		Addr:         address,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := s.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println("cmd listen error: ", err)
		}
	}()

}

func (s *cmdServer) shutdown() error {
	if s == nil || s.Server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.Server.Shutdown(ctx)
}
