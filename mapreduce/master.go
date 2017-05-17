package mapreduce

import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) sendJob(action JobType, jobNumber int, numotherphase int) {
		    worker := <-mr.registerChannel

			args := &DoJobArgs{mr.file, action, jobNumber, numotherphase}
			var reply DoJobReply
			ok := false
			for !ok {
				ok = call(worker, "Worker.DoJob", args, &reply)
				if ok == false {
					log.Printf("DoJob: RPC %s shutdown error\n", worker)
					worker = <-mr.registerChannel
				}
			}
			log.Println("Job", jobNumber, action, "completed")
			mr.registerChannel <- worker

            if action == Map {
                mr.finishMap <- "done"
			    log.Println("Job", jobNumber, "sent signal done")
            } else {
                mr.finishReduce <- "done"
            }

}


func (mr *MapReduce) RunMaster() *list.List {
	log.Println("RunMaster()")

        for i := 0; i < mr.nMap; i++ {
            go mr.sendJob(Map, i, mr.nReduce)
        }
        for i := 0; i < mr.nMap; i++ {
            <- mr.finishMap
            log.Println("completion num", i)
        }

        for i := 0; i < mr.nReduce; i++ {
            go mr.sendJob(Reduce, i, mr.nMap)
        }

        for i := 0; i < mr.nReduce; i++ {
            <- mr.finishReduce
            log.Println("completion num", i)
        }


        // wait for jobs to complete
	return mr.KillWorkers()
}
