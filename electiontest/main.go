package main

import (
	"fmt"
	"time"

	"election"
)

//角色变化才会调用该函数，onChangeToLeader 切换为leader 可以在这里进行数据初始化工作
func onChangeToLeader() {
	fmt.Println("change to  leader ++++++++++++++++++++++++++++")
}

func onChangeToCandidate() {
	fmt.Println("change to Candidate ---------------------------------------------")
}

func main() {
	host := []string{"192.168.1.76:2181", "192.168.1.77:2181", "192.168.1.78:2181"}
	Manager, _ := election.NewElectionManager(host, 3*time.Second, "/zhanyi/test", "leader_test", "1")
	_, err := Manager.Join(election.WithChangeToLeaderCallback(onChangeToLeader),
		election.WithChangeToCandidateCallback(onChangeToCandidate))
	if err != nil {
		fmt.Println("Join err ", err)
		return
	}
	/*ch, err := Manager.Join()
	if err != nil {
		fmt.Println("Join err ", err)
		return
	}
	go func() {
		fmt.Println("111111111111111111111")
		for {
			select {
			case c := <-ch:
				if c.Isleader {
					fmt.Println("change to leader ++++++++++++++++++++++++++++")")
				} else {
					fmt.Println("change to Candidate ---------------------------------------------")")
				}
			}
		}
	}()*/
	for {
		time.Sleep(3 * time.Second)
		leader := Manager.IsLeader()
		if leader {
			fmt.Println("now i am leader")
		} else {
			fmt.Println("now i am Candidate")
		}

	}

}
