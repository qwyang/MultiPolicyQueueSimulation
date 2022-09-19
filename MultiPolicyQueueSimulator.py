import numpy as np
import queue
class DescreteNumberGenerator(object):
    def rand(self,low,high,size):
        return  np.random.randint(low,high,size)
    def randnormal(self,mu,sigma,size):
        data = np.random.normal(loc=mu, scale=sigma, size=size)
        def positive(d):
            d = int(round(abs(d)))
            if d < 1:
                return 1
            return d
        return map(positive,data)
    def randexp(self,interval,size):
        data = np.random.exponential(interval,size)
        return map(int,map(round,data))

class EventType(object):
    SYS_BEGIN = 1
    SYS_END = 2
    NEW_CUSTOMER = 3
    CUSTOMER_LEAVE = 4

class Event(object):
    def __init__(self, time, etype, obj=None):
        self.time = time
        self.type = etype
        self.obj = obj
    def __lt__(self, other):
        return self.time < other.time
    
class Customer(object):
    counter = 0
    def __init__(self,arrivalTime,durationTime):
        self.arrivalTime = arrivalTime
        self.durationTime = durationTime
        self.startTime = 0
        self.servedTime = 0
        self.leaveTime = 0
        self.waitTime = 0
        Customer.counter += 1
        self.index = Customer.counter
        self.win = None   
    def setWindow(self, window):
        self.win = window
    def setStartTime(self, time):
        self.startTime = time
        self.waitTime = self.startTime - self.arrivalTime
        self.leaveTime = self.startTime + self.durationTime
    def __str__(self):
        return "arrive:{} start:{} duration:{} leave:{}".format(self.arrivalTime,self.startTime,self.durationTime,self.leaveTime)

class Window(object):
    counter = 0
    def __init__(self):
        self.que = list()
        self.busyTime = 0
        self.idleTime = 0
        self.lastFinishedTime = 0
        self.averageQueLength = 0
        self.servicedCount = 0
        Window.counter += 1
        self.index = Window.counter
    def queLength(self):
        return len(self.que)
    def remainServiceTime(self, currentTime):
        serviceTime = 0
        if self.que:
            serviceTime = self.que[0].startTime
            for c in self.que:
                serviceTime += c.durationTime
            serviceTime -= currentTime
        return serviceTime
    def addCustomer(self, c):
        currentLength = len(self.que)
        self.servicedCount += 1
        self.averageQueLength = (self.averageQueLength * (self.servicedCount-1) + currentLength)/ self.servicedCount
        self.que.append(c)
    def removeCustomer(self, c):
        self.que.remove(c)
    def nextCustomer(self):
        return self.que[0]
    def isFree(self):
        return len(self.que) == 0
    def startService(self, c, currentTime):
        self.idleTime += (currentTime - self.lastFinishedTime)
        self.lastFinishedTime = currentTime + c.durationTime
        c.setWindow(self)
        c.setStartTime(currentTime)
        self.busyTime += c.durationTime
    def __str__(self):
        return "window {}".format(self.index)
        
class PolicyType(object):
    CQ = "Classified Queue"
    FCFS = "FCFS"
    CM = "CALLER MACHINE"
    RANDOM = "RANDOM CHOICE"
    
class MultiWindowQueueSimulator(object):
    def __init__(self, intervalGen, durationGen, policy=PolicyType.RANDOM):
        self.clock = 0
        self.sysEndTime = 480
        self.windowNumber = 4
        self.eventList = queue.PriorityQueue()
        self.windows = [Window() for i in range(self.windowNumber)]
        self.totalWaitTime = 0
        self.longestWaitTime = 0
        self.customerNum = 0
        self.totalServiceTime = 0
        self.intervalGenerator = intervalGen
        self.serviceTimeGenerator = durationGen
        self.servedCustomers = []
        self.policy = policy
        self.serviceRatio = 0
        self.averageQueueLength = 0
    def generateNewCustomer(self):
        duration = next(self.serviceTimeGenerator)
        newCustomer = Customer(self.clock, duration)
        return newCustomer
    def selectWindow(self, policy):
        def selectByCQ():
            pass
        def selectByFCFS():
            sw = self.windows[0]
            for w in self.windows[1:]:
                if w.queLength() < sw.queLength():
                    sw = w
            return sw
        def selectByCM():
            sw = self.windows[0]
            for w in self.windows[1:]:
                if w.remainServiceTime(self.clock) < sw.remainServiceTime(self.clock):
                    sw = w
            return sw            
        def selectByRAND():
            n = np.random.randint(0,4)
            sw = self.windows[n]
            return sw
        funcs = {PolicyType.CQ:selectByCQ,
                 PolicyType.FCFS:selectByFCFS,
                 PolicyType.CM:selectByCM,
                 PolicyType.RANDOM:selectByRAND}
        return funcs[policy]()        
    def run(self):
        e = Event(0,EventType.SYS_BEGIN)
        self.eventList.put((e.time,e))
        while not self.eventList.empty():
            time,e = self.eventList.get(0)
            self.clock = time
            if e.type == EventType.SYS_BEGIN:
                self.eventList.put((0, Event(0,EventType.NEW_CUSTOMER)))
            elif e.type == EventType.NEW_CUSTOMER:
                self.customerNum += 1
                c = self.generateNewCustomer()
                #print("new",c)
                w = self.selectWindow(self.policy)
                if w.isFree():
                    w.addCustomer(c)
                    w.startService(c, self.clock)
                    e = Event(c.leaveTime, EventType.CUSTOMER_LEAVE, c)
                    self.eventList.put((e.time, e))
                else:
                    w.addCustomer(c)
                interval = next(self.intervalGenerator)
                if self.clock < self.sysEndTime:
                    e = Event(self.clock + interval, EventType.NEW_CUSTOMER)
                    self.eventList.put((e.time, e))
            elif e.type == EventType.CUSTOMER_LEAVE:
                c = e.obj
                #print(c,c.win)
                window = c.win
                window.removeCustomer(c)
                self.servedCustomers.append(c)
                if not window.isFree():
                    c = window.nextCustomer()
                    window.startService(c, self.clock)
                    e = Event(c.leaveTime, EventType.CUSTOMER_LEAVE, c)
                    self.eventList.put((e.time, e))
            else:
                pass
    def getResults(self):
        for c in self.servedCustomers:
            self.totalWaitTime += c.waitTime
            self.totalServiceTime += c.durationTime
            if c.waitTime > self.longestWaitTime:
                self.longestWaitTime = c.waitTime
        self.averageWaitTime = self.totalWaitTime/self.customerNum
        self.serviceRatio = sum((w.busyTime for w in self.windows))/ (sum((w.busyTime for w in self.windows))+sum((w.idleTime for w in self.windows)))
        self.averageQueueLength = sum((w.averageQueLength for w in self.windows))/4


class RepeatRunner(object):
    def __init__(self, repeat=50):
        results = []
        for policy in [PolicyType.FCFS,PolicyType.CM,PolicyType.RANDOM]:
            for i in range(repeat):
                averageIntervalTime = 3
                averageServiceTime = 15
                intervalGen = (i for i in DescreteNumberGenerator().rand(1,averageIntervalTime*2+1,1000))
                durationGen = (i for i in DescreteNumberGenerator().rand(1,averageServiceTime*2+1,1000))
                simulator = MultiWindowQueueSimulator(intervalGen,durationGen,policy)
                simulator.run()
                simulator.getResults()
                results.append(simulator)
            print("----------------------------Policy：{}-------------------------------------------".format(policy))
            print("average wait time: ",sum((s.averageWaitTime for s in results))/len(results))
            print("logest wait time: ",sum((s.longestWaitTime for s in results))/len(results))
            print("average customer number: ",sum((s.customerNum for s in results))/len(results))
            print("total service time: ",sum((s.totalServiceTime for s in results))/len(results))
            print("service ratio: ",sum((s.serviceRatio for s in results))/len(results))
            print("averageQueueLength:",sum((s.averageQueueLength for s in results))/len(results))
            print("windows 0 busy time: ",sum((s.windows[0].busyTime for s in results))/len(results))
            print("windows 0 idle time: ",sum((s.windows[0].idleTime for s in results))/len(results))
            print("window 0 served customer number: ",sum((s.windows[0].servicedCount for s in results))/len(results))
            print("window 0 averageQueLength: ",sum((s.windows[0].averageQueLength for s in results))/len(results))
            print("windows 1 busy time: ",sum((s.windows[1].busyTime for s in results))/len(results))
            print("windows 1 idle time: ",sum((s.windows[1].idleTime for s in results))/len(results))
            print("window 1 averageQueLength: ",sum((s.windows[1].averageQueLength for s in results))/len(results))
            print("window 1 served customer number: ",sum((s.windows[1].servicedCount for s in results))/len(results))
            print("windows 2 busy time: ",sum((s.windows[2].busyTime for s in results))/len(results))
            print("windows 2 idle time: ",sum((s.windows[2].idleTime for s in results))/len(results))
            print("window 2 averageQueLength: ",sum((s.windows[2].averageQueLength for s in results))/len(results))
            print("window 2 served customer number: ",sum((s.windows[2].servicedCount for s in results))/len(results))
            print("windows 3 busy time: ",sum((s.windows[3].busyTime for s in results))/len(results))
            print("windows 3 idle time: ",sum((s.windows[3].idleTime for s in results))/len(results))
            print("window 3 averageQueLength: ",sum((s.windows[3].averageQueLength for s in results))/len(results))
            print("window 3 served customer number: ",sum((s.windows[3].servicedCount for s in results))/len(results))
            
    
if __name__ == "__main__":
    RepeatRunner()
