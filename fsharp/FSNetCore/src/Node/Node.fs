namespace Node

open System
open NetworkServer.Communication
open NetworkServer.MessageSerialization
open System.Collections.Generic
open DataTypes
open FailureDetectors
open PingAckFailureDetector
open HeartbeatFailureDetector
open HeartbeatRecoveryFailureDetector
open HeartbeatSlidingWindowFailureDetector
open HeartbeatSuspectLevelFailureDetector
open Consensus
open GossippingFailureDetector

type NetworkProtocol =
    | TCP
    | UDP

type FailureDetectorType =
    | PingAck
    | SimpleHeartbeat
    | HeartbeatRecovery
    | HeartbeatSlidingWindow
    | HeartbeatSuspectLevel
    | NoFailureDetector

type ConsensusType =
    | ChandraToueg
    | NoConsensus

type NodeConfiguration = {
    neighbors: Neighbor list
    networkProtocol: NetworkProtocol
    failureDetector: FailureDetectorType
    consensus: ConsensusType
    receiveMessageFunction: obj -> Async<unit>
    gossipping: bool
    verbose: bool
}

type Node (host, port, value) =

    [<DefaultValue>] val mutable NetworkServer : NetworkServer
    [<DefaultValue>] val mutable FailureDetector : FailureDetector
    [<DefaultValue>] val mutable Consensus: ChandraTouegConsensus
    [<DefaultValue>] val mutable Neighbors : HashSet<Neighbor>
    [<DefaultValue>] val mutable receiveMessageUserFunction: obj -> Async<unit>

    member val id = Guid.NewGuid().ToString()

    member x.InitializeNode(conf: NodeConfiguration) = async {
        x.Neighbors <- new HashSet<Neighbor>()
        x.receiveMessageUserFunction <- conf.receiveMessageFunction

        for n in conf.neighbors do
            x.Neighbors.Add n |> ignore
            printfn "Added neighbor %s:%i" n.host n.port

        x.NetworkServer <-
            match conf.networkProtocol with
            | TCP -> TcpServer(port, x.ReceiveMessage) :> NetworkServer
            | UDP -> UdpServer(port, x.ReceiveMessage) :> NetworkServer

        x.FailureDetector <-
            let fd =
                match conf.failureDetector with
                | PingAck ->
                    PingAckFailureDetector(host, port, conf.verbose, x.DetectedFailure) :> FailureDetector
                | SimpleHeartbeat ->
                    HeartbeatFailureDetector(host, port, conf.verbose, x.DetectedFailure) :> FailureDetector
                | HeartbeatRecovery ->
                    HeartbeatRecoveryFailureDetector(host, port, conf.verbose, x.DetectedFailure) :> FailureDetector
                | HeartbeatSlidingWindow ->
                    HeartbeatSlidingWindowFailureDetector(host, port, conf.verbose, x.DetectedFailure) :> FailureDetector
                | HeartbeatSuspectLevel ->
                    HeartbeatSuspectLevelFailureDetector(host, port, conf.verbose, x.DetectedFailure) :> FailureDetector
                | NoFailureDetector -> failwith "No Failure Detector Specified"

            if conf.gossipping then GossippingFailureDetector (fd) :> FailureDetector else fd

        x.FailureDetector.InitializeFailureDetector x.NetworkServer x.Neighbors

        x.Consensus <-
            match conf.consensus with
            | ChandraToueg ->
                ChandraTouegConsensus(x.NetworkServer, { host = host; port = port }, x.Neighbors, value)
            | NoConsensus -> failwith "No Consensus Algorithm Specified"

        printfn "Initial value of %s:%i is %A." host port x.Consensus.Value
    }

    member x.ReceiveMessage (message: byte []) = async {
        let! receivedMessage = deserializeNewtonsoft message

        let! failureDetectorMessage = x.FailureDetector.ReceiveMessage receivedMessage x.AddNewNeighbor

        if not failureDetectorMessage then
            let! consensusMessage = x.Consensus.ReceiveMessage receivedMessage

            if not consensusMessage then
                do! x.receiveMessageUserFunction receivedMessage
    }

    member x.DetectedFailure (neighbor: Neighbor) = async {
        do! x.Consensus.DetectedFailure neighbor
    }

    /// Starts the server to listen for requests.
    member x.Start = async {
        printfn "Starting server..."
        do! x.NetworkServer.StartServer |> Async.StartChild |> Async.Ignore

        do! x.FailureDetector.DetectFailures |> Async.StartChild |> Async.Ignore
    }

    member x.AddNewNeighbor neighbor = async {
        x.Neighbors.Add neighbor |> ignore
        printfn "Node: Added Neighbor %s:%i" neighbor.host neighbor.port

        do! x.FailureDetector.AddNeighbor neighbor
    }

    member x.UpdateValue newValue = async {
        do! x.Consensus.UpdateValue newValue
    }

    member x.StartConsensus () = async {
        printfn "Starting Consensus"
        do! x.Consensus.StartConsensus ()
    }