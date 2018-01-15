package main

import (
    "fmt"
    "time"
    "net"
    "os"
    "strconv"
    "strings"
    msg "./message"
)


func stringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}

func updateProcList(list *string, id int){
    idS := strconv.Itoa(id)
    strList := strings.Split(*list, ".")

    if !stringInSlice(idS, strList) {
        *list = *list + "."+idS
    }
}

func maxProcID(list string) int {
    strList := strings.Split(list, ".")
    var max int
    for _, e := range strList {
        val, err := strconv.Atoi(e)
        if err  != nil {
          panic(err)
        }
        if val > max {
            max = val
        }
    }
    return max
}


func isProcInList(list string, procID int) bool {
    strList := strings.Split(list, ".")
    for _, e := range strList {
        val, err := strconv.Atoi(e)
        if err  != nil {
          panic(err)
        }
        if val == procID {
            return true
        }
    }
    return false
}




func manageConnection(Conn *net.UDPConn,
                dataCh chan msg.Message,
                timeoutCh chan time.Duration) {
    var buffer = make([]byte, 4096)
    var m msg.Message
    var timeout time.Duration
    timeout = time.Duration(0)

    for {
        select {
            case timeout = <-timeoutCh: {}
            default: {
                if timeout != 0 {
                    Conn.SetReadDeadline(time.Now().Add(timeout))
                }
                n,addr,err := Conn.ReadFromUDP(buffer)
                _ = addr
                if err != nil {
                    if e, ok := err.(net.Error); !ok || !e.Timeout() {
                        panic(err)
                    } else {
                        m = msg.Message{Type_: "timeout", Dst_: 0, Data_: ""}
                    }
                } else {
                    m = msg.FromJsonMsg(buffer[0:n])
                }
                dataCh <- m
            }
        }

    }

}



func processMyDataMsg(id int,
                      lId int,
                      rId int,
                      m msg.Message) msg.Message {
    m_data := msg.FromJsonDataMsg([]byte(m.Data_))
    switch m_data.Type_ {
        case "conf": {
            fmt.Println("node",
                        id,
                        ": received token from node",
                        lId,
                        "with delivery confirmation from node",
                        m_data.Src_,
                        ", sending token to node",
                        rId)
            init_data_m := msg.DataMessage{Type_: "empty", Dst_: -1, Data_: ""}
            m = msg.Message{Type_: "token", Dst_: -1, Data_: string(init_data_m.ToJsonDataMsg())}

        }
        case "send": {
            fmt.Println("node",
                        id,
                        ": received token from node",
                        lId,
                        "with data from node",
                        m_data.Src_,
                        "(data =`", m_data.Data_, "`),",
                        "sending token to node",
                        rId)
            confirmation_m := msg.DataMessage{Type_: "conf", Dst_: m_data.Src_, Src_: id, Data_: ""}
            m = msg.Message{Type_: "token", Dst_: m_data.Src_, Data_: string(confirmation_m.ToJsonDataMsg())}
        }
    }
    return m
}


func processEmptyTokenAndCheckMaintance(id int,
                                        lId int,
                                        rId int,
                                        m msg.Message,
                                        taskCh chan msg.Message,
                                        needDrop *bool) msg.Message {
    select {
        case tmp := <-taskCh:
            switch tmp.Type_ {
                case "send":
                    m_data := msg.DataMessage{Type_: "send", Dst_: tmp.Dst_, Src_: id, Data_: tmp.Data_}
                    m = msg.Message{Type_: "token", Dst_: tmp.Dst_, Data_: string(m_data.ToJsonDataMsg())}
                case "drop":
                    *needDrop = true
                default:
                    fmt.Println("Unknown maintance task!:", m.Type_)
            }
        default: {}
    }
    fmt.Println("node",
                id,
                ": received token from node",
                lId,
                ", sending token to node",
                rId)
    return m
}


func launchElection(id int, lId int, rId int,
                    connection *net.UDPConn, lAddr *net.UDPAddr, rAddr *net.UDPAddr,
                    electDoubled []bool, msgReturned *bool, generated *bool, noToken *bool,
                    delay time.Duration) {
    fmt.Println("node",
                id,
                ": received timeout, launching election")


    *noToken = true

    elect_m := msg.DataMessage{Type_: "elect", Dst_: rId, Src_: id, Data_: strconv.Itoa(id)}
    m := msg.Message{Type_: "elect", Dst_: id, Data_: string(elect_m.ToJsonDataMsg())}

    buffer := m.ToJsonMsg()
    time.Sleep(delay)
    _, err := connection.WriteToUDP(buffer, rAddr)
    if err  != nil {
      panic(err)
    }

    elect_m = msg.DataMessage{Type_: "elect", Dst_: lId, Src_: id, Data_: strconv.Itoa(id)}
    m = msg.Message{Type_: "elect", Dst_: id, Data_: string(elect_m.ToJsonDataMsg())}

    buffer = m.ToJsonMsg()
    time.Sleep(delay)
    _, err = connection.WriteToUDP(buffer, lAddr)
    if err  != nil {
      panic(err)
    }

    for i := range electDoubled {
        electDoubled[i] = false
    }
    electDoubled[id] = true
    *msgReturned = false
    *generated = false
}


func processElection(id int, lId int, rId int, nodes int,
                     connection *net.UDPConn, lAddr *net.UDPAddr, rAddr *net.UDPAddr,
                     m msg.Message,
                     electDoubled []bool,
                     noToken *bool, generated *bool, msgReturned *bool,
                     delay time.Duration) {

    var err error
    if m.Dst_ == id {
        if !*msgReturned {
            m_tmp := msg.FromJsonDataMsg(([]byte)(m.Data_))
            *msgReturned = true
            for i := 0; i < nodes; i++ {
                if !isProcInList(m_tmp.Data_, i) {
                    *msgReturned = false
                    break
                }
            }
        }
        if *msgReturned {
            m = msg.Message{Type_: "electfin", Dst_: id, Data_: m.Data_}

            buffer := m.ToJsonMsg()
            time.Sleep(delay)
            _, err = connection.WriteToUDP(buffer, rAddr)
            if err  != nil {
              panic(err)
            }
        }
    } else {
        fmt.Println("node", id, ": received election token", "with data:", m)
        if !*generated {
            *noToken = true
        }

        if !electDoubled[m.Dst_] {
            m_tmp := msg.FromJsonDataMsg([]byte(m.Data_))
            m_tmp.Src_ = id
            m_tmp.Dst_ = rId
            updateProcList(&m_tmp.Data_, id)
            m = msg.Message{Type_: "elect", Dst_: m.Dst_, Data_: string(m_tmp.ToJsonDataMsg())}

            buffer := m.ToJsonMsg()
            time.Sleep(delay)
            _, err = connection.WriteToUDP(buffer, rAddr)
            if err  != nil {
              panic(err)
            }

            m_tmp.Dst_ = lId
            m = msg.Message{Type_: "elect", Dst_: m.Dst_, Data_: string(m_tmp.ToJsonDataMsg())}

            buffer = m.ToJsonMsg()
            time.Sleep(delay)
            _, err = connection.WriteToUDP(buffer, lAddr)
            if err  != nil {
              panic(err)
            }

            electDoubled[m.Dst_] = true
        } else {
            m_tmp := msg.FromJsonDataMsg([]byte(m.Data_))

            if m_tmp.Src_ == lId {
                m_tmp.Src_ = id
                m_tmp.Dst_ = rId
                updateProcList(&m_tmp.Data_, id)
                m = msg.Message{Type_: "elect", Dst_: m.Dst_, Data_: string(m_tmp.ToJsonDataMsg())}

                buffer := m.ToJsonMsg()
                time.Sleep(delay)
                _, err = connection.WriteToUDP(buffer, rAddr)
                if err  != nil {
                  panic(err)
                }
            } else {
                m_tmp.Src_ = id
                m_tmp.Dst_ = lId
                updateProcList(&m_tmp.Data_, id)
                m = msg.Message{Type_: "elect", Dst_: m.Dst_, Data_: string(m_tmp.ToJsonDataMsg())}

                buffer := m.ToJsonMsg()
                time.Sleep(delay)
                _, err = connection.WriteToUDP(buffer, lAddr)
                if err  != nil {
                  panic(err)
                }
            }
        }
    }
}



func processElectionFinish(id int,
                           connection *net.UDPConn, rAddr *net.UDPAddr,
                           m msg.Message,
                           msgReturned *bool, generated *bool, noToken *bool,
                           delay time.Duration) {
    var err error
    var lastElectMsg msg.Message
    if *noToken {
        *msgReturned = true
        fmt.Println("node",
                    id,
                    ": received election token",
                    "with data:",
                    m)
        lastElectMsg = m
        if m.Dst_ != id {
            buffer := m.ToJsonMsg()
            time.Sleep(delay)
            _, err = connection.WriteToUDP(buffer, rAddr)
            if err  != nil {
              panic(err)
            }
        }
        max := maxProcID(msg.FromJsonDataMsg(([]byte)(lastElectMsg.Data_)).Data_)
        if max == id && !*generated {
            fmt.Println("node",
                        id,
                        ": generated token")
            m_tmp := msg.DataMessage{Type_: "empty", Dst_: -1, Data_: ""}
            m = msg.Message{Type_: "token", Dst_: -1, Data_: string(m_tmp.ToJsonDataMsg())}
            buffer := m.ToJsonMsg()

            time.Sleep(delay)
            _,err := connection.WriteToUDP(buffer, rAddr)
            if err  != nil {
              panic(err)
            }
            *generated = true
        }
        *noToken = false
    }
}





func tokenRing(id int,
          nodes int,
          portArr []int,
          maintArr []int,
          quitCh chan struct{},
          delay time.Duration) {

    port := portArr[id]
    maintPort := maintArr[id]
    lId := ((id-1)%nodes+nodes)%nodes
    rId := ((id+1)%nodes+nodes)%nodes
    lPort  := portArr[lId]
    rPort  := portArr[rId]

    addr,err := net.ResolveUDPAddr("udp","127.0.0.1:"+strconv.Itoa(port))
    if err  != nil {
      panic(err)
    }
    maintAddr,err := net.ResolveUDPAddr("udp","127.0.0.1:"+strconv.Itoa(maintPort))
    if err  != nil {
      panic(err)
    }

    lAddr,err := net.ResolveUDPAddr("udp","127.0.0.1:"+strconv.Itoa(lPort))
    if err  != nil {
      panic(err)
    }

    rAddr,err := net.ResolveUDPAddr("udp","127.0.0.1:"+strconv.Itoa(rPort))
    if err  != nil {
      panic(err)
    }

    connection, err := net.ListenUDP("udp", addr)
    if err  != nil {
      panic(err)
    }
    maintConnection, err := net.ListenUDP("udp", maintAddr)
    if err  != nil {
      panic(err)
    }

    if  id == 0 {
        buffer := make([]byte, 4096)

        init_data_m := msg.DataMessage{Type_: "empty", Dst_: -1, Data_: ""}
        data := string(init_data_m.ToJsonDataMsg())
        m := msg.Message{Type_: "token", Dst_: -1, Data_: data}
        buffer = m.ToJsonMsg()

        _,err := connection.WriteToUDP(buffer, rAddr)
        if err  != nil {
          panic(err)
        }

    }

    dataCh  := make(chan msg.Message)
    maintCh := make(chan msg.Message)
    taskCh := make(chan msg.Message, 4096)
    timeoutCh := make(chan time.Duration, 1)
    timeoutMaintCh := make(chan time.Duration, 1)

    timeoutCh <- delay * time.Duration(nodes) * 2
    timeoutMaintCh <- time.Second * 0

    go manageConnection(connection, dataCh, timeoutCh)
    go manageConnection(maintConnection, maintCh, timeoutMaintCh)

    buffer := make([]byte, 4096)
    var m msg.Message
    var needDrop bool
    needDrop = false

    var noToken bool
    noToken = false
    var generated bool
    generated = false
    electDoubled := make([]bool, nodes)
    for i := range electDoubled {
        electDoubled[i] = false
    }

    var msgReturned bool
    msgReturned = false

    for {
        select {
            case m = <- dataCh: {}
            case m = <- maintCh: {}
        }
        if m.Type_ == "token" {
            if !noToken {
                if m.Dst_ == id {

                    m = processMyDataMsg(id, lId, rId, m)

                } else if m.Dst_ == -1 {

                    m = processEmptyTokenAndCheckMaintance(id, lId, rId, m, taskCh, &needDrop)

                } else {
                    fmt.Println("node", id, ": received token from node with id = ", lId, ", sending token to node with id = ", rId)
                }

                if !needDrop {
                    buffer = m.ToJsonMsg()

                    time.Sleep(delay)
                    _, err = connection.WriteToUDP(buffer, rAddr)
                    if err  != nil {
                      panic(err)
                    }
                }
                needDrop = false

            } else {
                fmt.Println(id, " discarded token")
            }
        } else if m.Type_ == "timeout" {

            launchElection(id, lId, rId,
                           connection, lAddr, rAddr,
                           electDoubled, &msgReturned, &generated, &noToken,
                           delay)

        } else if m.Type_ == "elect" {
            processElection(id, lId, rId, nodes,
                            connection, lAddr, rAddr,
                            m,
                            electDoubled,
                            &noToken, &generated, &msgReturned,
                            delay)

        } else if m.Type_ == "electfin" {

            processElectionFinish(id,
                                  connection, rAddr,
                                  m,
                                  &msgReturned, &generated, &noToken,
                                  delay)
        } else {
            fmt.Println("node",
                        id,
                        ": received service message:",
                        string(m.ToJsonMsg()))
            switch m.Type_{
                case "send":
                    taskCh <- m
                case "drop":
                    taskCh <- m
                default:
                    fmt.Println("WTF")
            }
        }

    }

    <-quitCh
    quitCh <- struct{}{}
}


func main() {
    quitCh := make(chan struct{})
    nodes, _ := strconv.Atoi(os.Args[1])
    t, _ := strconv.Atoi(os.Args[2])
    time := time.Millisecond*time.Duration(t)

    portArr  := make([]int, nodes)
    maintArr := make([]int, nodes)
    for i := 0; i < nodes; i++ {
    	portArr[i] = 30000 + i
    	maintArr[i] = 40000 + i
    }

    for i := 0; i < nodes; i++ {
        go tokenRing(i, nodes, portArr, maintArr, quitCh, time)
    }

    for i := 0; i < nodes; i++ {
	    <-quitCh
	   }

}
