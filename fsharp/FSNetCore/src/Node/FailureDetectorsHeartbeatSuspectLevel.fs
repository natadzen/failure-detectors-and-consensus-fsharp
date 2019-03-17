namespace Node

open System
open DataTypes
open System.Collections.Generic
open NetworkServer.Communication
open FailureDetectors

module HeartbeatSuspectLevelFailureDetector =

    type Heartbeat = {
        messageId: string
        senderHost: string
        senderPort: int
    }

    type LastReceivedHeartbeatTime = int64

    type HeartbeatInfo() =

        let startingRoundtripTime = 2000L
        let slidingWindowSize = 50
        let mutable suspectLevel = 0
        let rwl = new System.Threading.ReaderWriterLockSlim()

        let averageRoundtripDuration (roundtrips: seq<LastReceivedHeartbeatTime>) =
            Seq.sum roundtrips / int64 (Seq.length roundtrips)

        member val lastReceivedHeartbeatTime =
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() with get, set

        member val roundtripTimes: LastReceivedHeartbeatTime list =
            [startingRoundtripTime] with get, set

        member x.AddRoundtrip (time: LastReceivedHeartbeatTime) =
            x.roundtripTimes <- time::x.roundtripTimes
            // printfn "ADDED NEW ROUNDTRIP! NOW AVERAGE IS: %i" (x.AcceptableRoundtripTime ())

        member x.AcceptableRoundtripTime () =
            if Seq.length x.roundtripTimes <= slidingWindowSize then
                x.roundtripTimes |> averageRoundtripDuration
            else
                Seq.windowed slidingWindowSize x.roundtripTimes
                |> Seq.head
                |> averageRoundtripDuration

        member x.UpdateLastReceivedHeartbeatTime time =
            x.lastReceivedHeartbeatTime <- time

        member x.SuspectLevel
            with get () =
                rwl.EnterReadLock()
                try suspectLevel finally rwl.ExitReadLock()
            and set (value) =
                rwl.EnterWriteLock()
                try suspectLevel <- value finally rwl.ExitWriteLock()

        member x.ReduceSuspicion () =
            if x.SuspectLevel > 0 then
                x.SuspectLevel <- x.SuspectLevel - 1


    type HeartbeatSuspectLevelFailureDetector (host, port, verbose, detectedFailureFunction) =
        inherit FailureDetector ()

        [<DefaultValue>] val mutable server : NetworkServer
        [<DefaultValue>] val mutable Neighbors : HashSet<Neighbor>
        [<DefaultValue>] val mutable HeartbeatsInfo : Dictionary<Neighbor, HeartbeatInfo>

        member val heartbeatInterval = 2000
        member val failureDetectionInterval = 4000
        member val suspectLevelMaximum = 3

        /// Collection of neighbors that are suspected to have failed.
        member val Suspected = new Dictionary<Neighbor, LastReceivedHeartbeatTime>()

        override x.GetSuspectedList = async {
            return Seq.toList x.Suspected.Keys
        }

        override x.InitializeFailureDetector (server: NetworkServer) (neighbors: HashSet<Neighbor>) =
            x.server <- server
            x.Neighbors <- neighbors
            x.HeartbeatsInfo <- new Dictionary<Neighbor, HeartbeatInfo>()
            for n in neighbors do
                do x.HeartbeatsInfo.Add(n, HeartbeatInfo())

        override x.ReceiveMessage message updateNeighborsFunction = async {
            match message with
            | :? Heartbeat as heartbeat ->
                do! x.HandleHeartbeat heartbeat updateNeighborsFunction
                return true
            | _ ->
                return false
        }

        override x.DetectFailures = async {
            do! x.ReportHealthWorkflow |> Async.StartChild |> Async.Ignore

            do! x.DetectFailuresWorkflow |> Async.StartChild |> Async.Ignore
        }

        override x.AddNeighbor neighbor = async {
            x.Neighbors.Add neighbor |> ignore

            let heartbeatInfo = HeartbeatInfo()
            let currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            heartbeatInfo.UpdateLastReceivedHeartbeatTime currentTime

            x.HeartbeatsInfo.Add(neighbor, heartbeatInfo)
            if verbose then printfn "Failure Detector: Added Neighbor %s:%i." neighbor.host neighbor.port
        }

        override x.AddSuspects neighbors = async {
            for n in neighbors do
                if n.host <> host && n.port <> port && not <| x.Suspected.ContainsKey n then
                    do! x.NeighborIsDown n
        }

        member x.ReportHealthWorkflow = async {
            if verbose then printfn "Set up heartbeat schedule for neighbors."
            while true do
                try
                    // Waiting a number of milliseconds before sending heartbeats again
                    do! Async.Sleep x.heartbeatInterval

                    // Pinging each not suspected neighbor
                    let notSuspectedNeighbors = new HashSet<Neighbor>(x.Neighbors)
                    notSuspectedNeighbors.ExceptWith(x.Suspected.Keys)

                    for n in notSuspectedNeighbors do
                        do! x.SendHeartbeat n
                with
                | ex->
                    if verbose then printf "Report Health Workflow Exception: %s, %A" ex.Message ex
        }

        member x.DetectFailuresWorkflow = async {
            if verbose then printfn "Set up failure detection."
            while true do
                try
                    // Waiting failureDetectionInterval milliseconds
                    do! Async.Sleep x.failureDetectionInterval

                    // Detecting failure
                    for heartbeatInfo in x.HeartbeatsInfo do
                        let neighbor = heartbeatInfo.Key
                        if not <| x.Suspected.ContainsKey neighbor then
                            let lastHeartbeatTime = heartbeatInfo.Value.lastReceivedHeartbeatTime
                            let currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                            let timeSinceLastHeartbeat = currentTime - lastHeartbeatTime

                            let numberOfLostHeartbeats = int (timeSinceLastHeartbeat / heartbeatInfo.Value.AcceptableRoundtripTime ())

                            if numberOfLostHeartbeats > 0 then
                                x.HeartbeatsInfo.[neighbor].SuspectLevel <- numberOfLostHeartbeats
                                if numberOfLostHeartbeats >= x.suspectLevelMaximum then
                                    do! x.NeighborIsDown neighbor
                                else
                                    if verbose then printfn "Neighbor %s:%i is suspected at level '%i'." neighbor.host neighbor.port x.HeartbeatsInfo.[neighbor].SuspectLevel
                with
                | ex ->
                    if verbose then printf "Detect Failures Workflow Exception: %s" ex.Message
        }

        // Nodes only send pings to neighbors
        member x.SendHeartbeat (neighbor: Neighbor) = async {

            // Preparing the Heartbeat message
            let heartbeatMessage: Heartbeat = {
                messageId = System.Guid.NewGuid().ToString();
                senderHost = host;
                senderPort = port;
            }

            // Sending the Heartbeat message
            do! x.server.SendMessage heartbeatMessage neighbor.host neighbor.port

            if verbose then printfn "Sent Heartbeat %s to %s:%i.\n" heartbeatMessage.messageId neighbor.host neighbor.port
        }

        member x.HandleHeartbeat (heartbeat: Heartbeat) updateNeighborsFunction = async {

            if verbose then printfn "Received Heartbeat from %s:%i with id %s.\n" heartbeat.senderHost heartbeat.senderPort heartbeat.messageId
            let neighbor = {host = heartbeat.senderHost ; port = heartbeat.senderPort}
            let heartbeatReceivedTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

            // If we were previously suspecting the node which sent the heartbeat,
            // remove it from suspects as it came back up
            if x.Suspected.ContainsKey(neighbor) then
                do! x.NeighborCameBackUp neighbor heartbeatReceivedTime
                x.HeartbeatsInfo.[neighbor].UpdateLastReceivedHeartbeatTime heartbeatReceivedTime

            // Heartbeat from an existing neighbor
            elif x.Neighbors.Contains neighbor then
                let heartbeatRoundtrip =
                    heartbeatReceivedTime - x.HeartbeatsInfo.[neighbor].lastReceivedHeartbeatTime
                x.HeartbeatsInfo.[neighbor].AddRoundtrip heartbeatRoundtrip
                if verbose then printfn "ADDED NEW ROUNDTRIP! NOW AVERAGE IS: %i" (x.HeartbeatsInfo.[neighbor].AcceptableRoundtripTime ())
                x.HeartbeatsInfo.[neighbor].UpdateLastReceivedHeartbeatTime heartbeatReceivedTime

            // Heartbeat from an new node
            // Add the node to the list of neighbors
            else
                do! updateNeighborsFunction neighbor

            x.HeartbeatsInfo.[neighbor].ReduceSuspicion()
        }

        member x.NeighborIsDown neighbor = async {
            if verbose then printfn "SUSPECTED FAILURE OF NEIGHBOR %s:%i AT MAXIMUM LEVEL." neighbor.host neighbor.port
            x.Suspected.Add(neighbor, x.HeartbeatsInfo.[neighbor].lastReceivedHeartbeatTime)
            do! detectedFailureFunction neighbor
        }

        member x.NeighborCameBackUp neighbor heartbeatReceivedTime = async {
            // New roundtrip time will be longer
            // based on how long it took the recovered node to come back
            let heartbeatRoundtrip = heartbeatReceivedTime - x.Suspected.[neighbor]

            x.Suspected.Remove neighbor |> ignore

            if verbose then printfn "NEIGHBOR %s:%i CAME BACK UP. OLD ROUNDTRIP TIME: %i. NEW ROUNDTRIP TIME: %i" neighbor.host neighbor.port (x.HeartbeatsInfo.[neighbor].AcceptableRoundtripTime()) heartbeatRoundtrip

            x.HeartbeatsInfo.[neighbor].AddRoundtrip heartbeatRoundtrip
            if verbose then printfn "ADDED NEW ROUNDTRIP! NOW AVERAGE IS: %i" (x.HeartbeatsInfo.[neighbor].AcceptableRoundtripTime ())
        }