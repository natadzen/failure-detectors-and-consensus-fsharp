namespace Node

open System
open DataTypes
open System.Collections.Generic
open NetworkServer.Communication
open FailureDetectors

module HeartbeatRecoveryFailureDetector =

    type Heartbeat = {
        messageId: string
        senderHost: string
        senderPort: int
    }

    type HeartbeatInfo() =

        member val roundtripTime = 500L with get, set

        member val lastReceivedHeartbeatTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() with get, set

        member x.UpdateLastReceivedHeartbeatTime time =
            x.lastReceivedHeartbeatTime <- time

        member x.UpdateRoundtripTime time =
            x.roundtripTime <- time

    type LastReceivedHeartbeatTime = int64

    type HeartbeatRecoveryFailureDetector (host, port, verbose, detectedFailureFunction) =
        inherit FailureDetector ()

        [<DefaultValue>] val mutable server : NetworkServer
        [<DefaultValue>] val mutable Neighbors : HashSet<Neighbor>
        [<DefaultValue>] val mutable HeartbeatsInfo : Dictionary<Neighbor, HeartbeatInfo>

        member val heartbeatInterval = 2000
        member val failureDetectionInterval = 4000

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
            heartbeatInfo.UpdateLastReceivedHeartbeatTime(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())

            x.HeartbeatsInfo.Add(neighbor, heartbeatInfo)
            printfn "Failure Detector: Added Neighbor %s:%i." neighbor.host neighbor.port
        }

        override x.AddSuspects neighbors = async {
            let currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            for n in neighbors do
                if n.host <> host && n.port <> port && not <| x.Suspected.ContainsKey n then
                    do! x.NeighborIsDown n currentTime
        }

        member x.ReportHealthWorkflow = async {
            printfn "Set up heartbeat schedule for neighbors."
            try
                while true do
                    // Waiting a number of milliseconds before sending heartbeats again
                    do! Async.Sleep x.heartbeatInterval

                    // Pinging each not suspected neighbor
                    let notSuspectedNeighbors = new HashSet<Neighbor>(x.Neighbors)
                    notSuspectedNeighbors.ExceptWith(x.Suspected.Keys)

                    for n in notSuspectedNeighbors do
                        do! x.SendHeartbeat n
            with
            | ex->
                printfn "Report Health Workflow Exception: %s" ex.Message
        }

        member x.DetectFailuresWorkflow = async {
            printfn "Set up failure detection."
            try
                while true do
                    // Waiting failureDetectionInterval milliseconds
                    do! Async.Sleep x.failureDetectionInterval

                    // Detecting failure
                    for heartbeatInfo in x.HeartbeatsInfo do
                        let neighbor = heartbeatInfo.Key
                        if not <| x.Suspected.ContainsKey neighbor then
                            let lastHeartbeatTime= heartbeatInfo.Value.lastReceivedHeartbeatTime
                            let currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                            let timeSinceLastHeartbeat = currentTime - lastHeartbeatTime

                            let acceptableHeartbeatRoundtripTime = heartbeatInfo.Value.roundtripTime
                            let acceptableTimeSincePreviousHeartbeat = acceptableHeartbeatRoundtripTime + int64 x.heartbeatInterval

                            if timeSinceLastHeartbeat > acceptableTimeSincePreviousHeartbeat then
                                do! x.NeighborIsDown neighbor currentTime

            with
            | ex->
                printfn "Detect Failures Workflow Exception: %s" ex.Message
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

            printfn "Sent Heartbeat %s to %s:%i.\n" heartbeatMessage.messageId neighbor.host neighbor.port
        }

        member x.HandleHeartbeat (heartbeat: Heartbeat) updateNeighborsFunction = async {

            printfn "Received Heartbeat from %s:%i with id %s.\n" heartbeat.senderHost heartbeat.senderPort heartbeat.messageId
            let neighbor = {host = heartbeat.senderHost ; port = heartbeat.senderPort}

            // If we were previously suspecting the node which sent the heartbeat,
            // remove it from suspects as it came back up
            if x.Suspected.ContainsKey(neighbor) then
                do! x.NeighborCameBackUp neighbor
            // Heartbeat from an existing neighbor
            elif x.Neighbors.Contains neighbor then
                x.HeartbeatsInfo.[neighbor].UpdateLastReceivedHeartbeatTime(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
            // Heartbeat from an new node
            // Add the node to the list of neighbors
            else
                do! updateNeighborsFunction neighbor
        }

        member x.NeighborIsDown neighbor currentTime = async {
            printfn "SUSPECTED FAILURE OF NEIGHBOR %s:%i" neighbor.host neighbor.port
            x.Suspected.Add(neighbor, currentTime)
            do! detectedFailureFunction neighbor
        }

        member x.NeighborCameBackUp neighbor = async {
            // New roundtrip time will be longer based on how long it took the recovered node to come back
            let newRoundtripTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - x.HeartbeatsInfo.[neighbor].lastReceivedHeartbeatTime

            x.Suspected.Remove neighbor |> ignore

            printfn "NEIGHBOR %s:%i CAME BACK UP. OLD ROUNDTRIP TIME: %i. NEW ROUNDTRIP TIME: %i" neighbor.host neighbor.port x.HeartbeatsInfo.[neighbor].roundtripTime newRoundtripTime

            x.HeartbeatsInfo.[neighbor].UpdateRoundtripTime(newRoundtripTime)
        }
