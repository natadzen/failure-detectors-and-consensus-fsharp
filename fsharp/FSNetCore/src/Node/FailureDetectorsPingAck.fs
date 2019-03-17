namespace Node

open System
open DataTypes
open System.Collections.Generic
open NetworkServer.Communication
open FailureDetectors

module PingAckFailureDetector =

    type Ping = {
        messageId: string
        senderHost: string
        senderPort: int
    }

    type AckForMessage = {
        messageId: string
        inResponse: string
        senderHost: string
        senderPort: int
    }


    type NodeHealthStatus() =
        member val lastSentPingTime = 0L with get, set
        member val lastReceivedAckTime = 0L with get, set

        member x.UpdateLastSentPingTime() =
            x.lastSentPingTime <- DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

        member x.UpdateLastReceivedAckTime() =
            x.lastReceivedAckTime <- DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()


    type PingAckFailureDetector (host, port, verbose, detectedFailureFunction) =
        inherit FailureDetector ()

        [<DefaultValue>] val mutable server : NetworkServer
        [<DefaultValue>] val mutable Neighbors : HashSet<Neighbor>
        [<DefaultValue>] val mutable NeighborsHealth : Dictionary<Neighbor, NodeHealthStatus>

        member val pingInterval = 4000
        member val failureDetectionInterval = 6000
        member val tolerateFailureFor = 10000L

        /// Collection of neighbors that are suspected to have failed.
        member val Suspected = new HashSet<Neighbor>()

        override x.GetSuspectedList = async {
            return Seq.toList x.Suspected
        }

        override x.InitializeFailureDetector (server: NetworkServer) (neighbors: HashSet<Neighbor>) =
            x.server <- server
            x.Neighbors <- neighbors
            x.NeighborsHealth <- new Dictionary<Neighbor, NodeHealthStatus>()
            for n in neighbors do x.NeighborsHealth.Add(n, NodeHealthStatus())

        override x.ReceiveMessage message updateNeighborsFunction = async {
            match message with
            | :? AckForMessage as ack ->
                do! x.HandleAck ack updateNeighborsFunction
                return true
            | :? Ping as ping ->
                do! x.HandlePing ping updateNeighborsFunction
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
            x.NeighborsHealth.Add(neighbor, NodeHealthStatus())
            printfn "Failure Detector: Added Neighbor %s:%i." neighbor.host neighbor.port
        }

        override x.AddSuspects neighbors = async {
            for n in neighbors do
                if n.host <> host && n.port <> port && not <| x.Suspected.Contains n then
                    printfn "SUSPECTED FAILURE OF NEIGHBOR %s:%i" n.host n.port
                    x.Suspected.Add n |> ignore
                    do! detectedFailureFunction n
        }

        member x.ReportHealthWorkflow = async {
            printfn "Set up pinging schedule for neighbors."
            try
                while true do
                    // Waiting a number of milliseconds before pinging neighbors again
                    do! Async.Sleep x.pingInterval

                    // Pinging each not suspected neighbor
                    let notSuspectedNeighbors = new HashSet<Neighbor>(x.Neighbors)
                    notSuspectedNeighbors.ExceptWith(x.Suspected)

                    for n in notSuspectedNeighbors do
                        do! x.SendPing n
            with
            | ex ->
                printfn "Report Health Workflow Exception: %s" ex.Message
        }

        member x.DetectFailuresWorkflow = async {
            printfn "Set up failure detection"
            try
                while true do
                    // Waiting failureDetectionInterval milliseconds
                    do! Async.Sleep x.failureDetectionInterval

                    // Detecting failure
                    for nh in x.NeighborsHealth do
                        let neighbor = nh.Key
                        let health = nh.Value

                        let howLongAckTook = health.lastReceivedAckTime - health.lastSentPingTime

                        if Math.Abs howLongAckTook > x.tolerateFailureFor then
                            printfn "SUSPECTED FAILURE OF NEIGHBOR %s:%i" neighbor.host neighbor.port
                            x.Suspected.Add neighbor |> ignore
                            do! detectedFailureFunction neighbor
                        elif x.Suspected.Contains neighbor then
                            printfn "NEIGHBOR CAME BACK %s:%i" neighbor.host neighbor.port
                            x.Suspected.Remove neighbor |> ignore
            with
            | ex ->
                printfn "Detect Failures Workflow Exception: %s" ex.Message
        }

        // Nodes only send pings to neighbors
        member x.SendPing (neighbor: Neighbor) = async {

            // Preparing the ping message
            let pingMessage: Ping = {
                messageId = System.Guid.NewGuid().ToString();
                senderHost = host;
                senderPort = port;
            }

            // Sending the ping message
            do! x.server.SendMessage pingMessage neighbor.host neighbor.port

            x.NeighborsHealth.[neighbor].UpdateLastSentPingTime()

            printfn "Sent Ping %s to %s:%i.\n" pingMessage.messageId neighbor.host neighbor.port
        }

        // Nodes only send acks to neighbors
        member x.SendAck (neighbor: Neighbor) (pingM: Ping) = async {

            // Preparing the acks message
            let ackMessage: AckForMessage = {
                messageId = System.Guid.NewGuid().ToString()
                inResponse = pingM.messageId
                senderHost = host
                senderPort = port
            }

            // Sending the ack message
            do! x.server.SendMessage ackMessage neighbor.host neighbor.port

            printfn "Sent Ack %s to %s:%i for message %s.\n" ackMessage.messageId neighbor.host neighbor.port ackMessage.inResponse
        }

        member x.HandlePing (ping: Ping) updateNeighborsFunction = async {

            printfn "Received Ping from %s:%i with id %s.\n" ping.senderHost ping.senderPort ping.messageId
            let neighbor = {host = ping.senderHost ; port = ping.senderPort}

            if not <| x.Neighbors.Contains neighbor then
                do! updateNeighborsFunction neighbor

            if x.Suspected.Contains neighbor then
                x.Suspected.Remove neighbor |> ignore

            // Sending the ack message for the ping
            do! x.SendAck neighbor ping
        }

        member x.HandleAck (ack: AckForMessage) updateNeighborsFunction = async {

            printfn "Received Ack from %s:%i with id %s for the message %s.\n" ack.senderHost ack.senderPort ack.messageId ack.inResponse

            let neighbor = {host = ack.senderHost; port = ack.senderPort}

            if x.Neighbors.Contains neighbor then
                x.NeighborsHealth.[neighbor].UpdateLastReceivedAckTime()
            else
                do! updateNeighborsFunction neighbor

            if x.Suspected.Contains neighbor then
                x.Suspected.Remove neighbor |> ignore
        }