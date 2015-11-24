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
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
    DPrintf("Run Master\n")
    mr.Workers = make(map[string]*WorkerInfo)
    for i := 0; i < 2; i++ {
        workInfo := &WorkerInfo{}
        register :=  <- mr.registerChannel
        workInfo.address = register
        mr.Workers[workInfo.address] = workInfo
    }

    for _, v := range mr.Workers {
        for i := 0; i < mr.nMap; i++ {
            arg := &DoJobArgs{}
            arg.File = mr.file
            arg.Operation = "Map"
            arg.JobNumber = i
            arg.NumOtherPhase = mr.nReduce
            res := DoJobReply{}
            ok := call(v.address, "Worker.DoJob", arg, &res)
            if ok == false {
                fmt.Printf("RPC Call DoJob error")
            }
         }
    }
    for _, v := range mr.Workers {
        for i := 0; i < mr.nReduce; i++ {
            arg := &DoJobArgs{}
            arg.File = mr.file
            arg.Operation = "Reduce"
            arg.JobNumber = i
            arg.NumOtherPhase = mr.nMap
            res := DoJobReply{}
            ok := call(v.address, "Worker.DoJob", arg, &res)
            if ok == false {
                fmt.Printf("RPC Call DoJob error")
            }
         }
    }
    return mr.KillWorkers()
}
