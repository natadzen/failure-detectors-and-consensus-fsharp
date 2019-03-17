namespace Node

open System
open DataTypes
open System.Collections.Generic
open NetworkServer.Communication

module Consensus =

    type ConsensusPreference = {
        round: int
        preference: obj
        timestamp: int64
    }

    type ConsensusCoordinatorPreference = {
        round: int
        preference: obj
    }

    type ConsensusPositiveAcknowledgement = {
        round: int
    }

    type ConsensusNegativeAcknowledgement = {
        round: int
    }

    type ConsensusDecide = {
        preference: obj
    }

    type RequestConsensus = {
        round: int
    }

    type ChandraTouegConsensus (server: NetworkServer, node, neighbors, initialValue) =

        member val server = server

        member val Node: Neighbor = node
        member val Neighbors: HashSet<Neighbor> = neighbors

        member val Value = initialValue with get, set

        member val Round = 0 with get, set
        member val Decision = initialValue with get, set
        member val ReceivedPreference = new Dictionary<int,ConsensusPreference list>() with get, set
        member val ReceivedPositiveAcknowledgement = new Dictionary<int,ConsensusPositiveAcknowledgement list>() with get, set
        member val ReceivedNegativeAcknowledgement = new Dictionary<int,ConsensusNegativeAcknowledgement list>() with get, set

        member x.GetCoordinator round =
            let coordinatorIndex = round % (x.Neighbors.Count + 1)
            let orderedNodes = Seq.sort (x.Node::List.ofSeq x.Neighbors)
            let coordinator = orderedNodes |> Seq.item coordinatorIndex
            coordinator

        member x.ReceiveMessage (message: obj) = async {
            printfn "RECEIVED CONSENSUS MESSAGE"
            match message with
            | :? ConsensusPreference as preference ->
                printfn "Consensus Preference: %A" preference
                do! x.HandlePreference preference
                return true
            | :? ConsensusCoordinatorPreference as coordinatorPreference ->
                printfn "Coordinator Preference: %A" coordinatorPreference
                do! x.HandleCoordinatorPreference coordinatorPreference
                return true
            | :? ConsensusPositiveAcknowledgement as ack ->
                printfn "Positive Acknowledgement: %A" ack
                do! x.HandlePositiveAcknowledgement ack
                return true
            | :? ConsensusNegativeAcknowledgement as nack ->
                printfn "Negative Acknowledgement: %A" nack
                do! x.HandleNegativeAcknowledgement nack
                return true
            | :? ConsensusDecide as decision ->
                printfn "Consensus Decide: %A" decision
                do! x.HandleDecide decision
                return true
            | :? RequestConsensus ->
                printfn "Consensus Requested"
                do! x.StartConsensus ()
                return true
            | _ -> return false
        }

        // ConsensusPreference can be sent by each node and received by the Coordinator.
        member x.HandlePreference (preference: ConsensusPreference) = async {
            printfn "Consensus Preference: %A" preference

            // If a node received ConsensusPreference message, it means others see it as Coordinator.
            // ACT 1: Record message in the dictionary with key: round, and value: list of consensusPreference messages.

            if x.ReceivedPreference.ContainsKey preference.round then
                x.ReceivedPreference.[preference.round] <- preference::x.ReceivedPreference.[preference.round]
            else
                x.ReceivedPreference.[preference.round] <- [preference]

            // Check if it received ConsensusPreference messages from majority of nodes
            // ACT 2: Check how many ConsensusPreference messages have already been received from this round.
            //        If count is more than or equal to majority of nodes, then proceed to ACT 3.
            //        If not, do nothing else.

            if x.ReceivedPreference.[preference.round].Length >= ((x.Neighbors.Count + 1) / 2) + 1 then
                // Received ConsensusPreference messages from majority of nodes.
                // ACT 3: Get the ConsensusPreference message with the latest timestamp.
                //        Send ConsensusCoordinatorPreference message to everyone.

                let latestTimestampPreference =
                    x.ReceivedPreference.[preference.round]
                    |> List.maxBy (fun p -> p.timestamp)

                let coordinatorPreference: ConsensusCoordinatorPreference =
                    {round = latestTimestampPreference.round; preference = latestTimestampPreference.preference}

                for n in x.Neighbors do
                    //printfn "Sent COORDINATOR PREFERENCE TO %s:%i" n.host n.port
                    do! x.server.SendMessage coordinatorPreference n.host n.port

                do! x.HandleCoordinatorPreference coordinatorPreference
        }

        member x.HandleCoordinatorPreference (coordinatorPreference: ConsensusCoordinatorPreference) = async {
            printfn "Coordinator Preference: %A" coordinatorPreference

            // ACT 0: Set ROUND value to the received round

            // ACT 1: Send ConsensusPositiveAcknowledgement to coordinator.
            //        (Compute who Coordinator is by R % N)

            let ack: ConsensusPositiveAcknowledgement =
                {round = coordinatorPreference.round}

            let coordinator = x.GetCoordinator coordinatorPreference.round

            if coordinator = x.Node then
                do! x.HandlePositiveAcknowledgement ack
            else
                do! x.server.SendMessage ack coordinator.host coordinator.port

            // ACT 2: Set Decision value to the Coordinator Preference.

            x.Decision <- coordinatorPreference.preference
        }

        member x.HandlePositiveAcknowledgement (ack: ConsensusPositiveAcknowledgement) = async {
            printfn "Positive Acknowledgement: %A" ack

            // If a node received ConsensusPositiveAcknowledgement message from a node,
            // it means that node sees it as Coordinator, and agrees to the preference
            // previously sent by Coordinator

            // ACT 1: Record message in the list of ConsensusPositiveAcknowledgement messages.

            if x.ReceivedPositiveAcknowledgement.ContainsKey ack.round then
                x.ReceivedPositiveAcknowledgement.[ack.round] <- ack::x.ReceivedPositiveAcknowledgement.[ack.round]
            else
                x.ReceivedPositiveAcknowledgement.[ack.round] <- [ack]

            // ACT 2: Check how many ConsensusPositiveAcknowledgement messages
            //        have already been received have already been received in this round.
            //        If count is more than or equal to majority of nodes, then proceed to ACT 3.
            //        If not, do nothing else.

            if x.ReceivedPositiveAcknowledgement.[ack.round].Length >= ((x.Neighbors.Count + 1) / 2) + 1 then
                // Received ConsensusPreference messages from majority of nodes.
                // ACT 3: Send ConsensusDecide message to everyone.

                let latestTimestampPreference =
                    x.ReceivedPreference.[ack.round]
                    |> List.maxBy (fun p -> p.timestamp)

                let decide: ConsensusDecide = { preference = latestTimestampPreference }

                for n in x.Neighbors do
                    do! x.server.SendMessage decide n.host n.port

                do! x.HandleDecide decide
                printfn "Decision has been made: %A" decide
        }

        member x.HandleNegativeAcknowledgement (nack: ConsensusNegativeAcknowledgement) = async {
            printfn "Negative Acknowledgement: %A" nack

            // If a node received ConsensusNegativeAcknowledgement message from a node,
            // it means that node thinks that this node (Coordinator) is down.

            // ACT 1: Record message in the list of ConsensusNegativeAcknowledgement messages.

            if x.ReceivedNegativeAcknowledgement.ContainsKey nack.round then
                x.ReceivedNegativeAcknowledgement.[nack.round] <- nack::x.ReceivedNegativeAcknowledgement.[nack.round]
            else
                x.ReceivedNegativeAcknowledgement.[nack.round] <- [nack]

            // ACT 2: Check how many ConsensusNegativeAcknowledgement messages
            //        have already been received have already been received in this round.
            //        If count is more than or equal to majority of nodes, then it means
            //        the decision will probably be made by some other coordinator, proceed to ACT 3.

            if x.ReceivedNegativeAcknowledgement.[nack.round].Length >= ((x.Neighbors.Count + 1) / 2) + 1 then
                // ACT 3: Clear all the state.

                do! x.ClearState ()
        }

        member x.HandleDecide (decision: ConsensusDecide) = async {
            // ACT 1: Set the Value to Decide value.

            x.Value <- decision.preference
            printfn "Decision accepted: %A" decision

            // ACT 2: Clear all the state.

            do! x.ClearState ()
        }

        member x.DetectedFailure (neighbor: Neighbor) = async {
            printfn "Consensus: Failure of node %s:%i detected." neighbor.host neighbor.port

            // ACT 0: Check if the crashed neighbor is a Coordinator

            if x.GetCoordinator x.Round = neighbor then
                // ACT 1: Send ConsensusNegativeAcknowledgement to Coordinator

                let nack: ConsensusNegativeAcknowledgement =
                    {round = x.Round}

                // If this node received a notification of failure, it itself didn't fail
                do! x.server.SendMessage nack neighbor.host neighbor.port

                // ACT 2: Increase round number and propose the value to the new Coordinator
                do! x.StartConsensus ()
        }

        member x.ClearState () = async {
            x.Round <- 0
            x.ReceivedPreference <- new Dictionary<int, ConsensusPreference list>()
            x.ReceivedPositiveAcknowledgement <- new Dictionary<int, ConsensusPositiveAcknowledgement list>()
            x.ReceivedNegativeAcknowledgement <- new Dictionary<int, ConsensusNegativeAcknowledgement list>()
        }

        member x.StartConsensus () = async {
            x.Round <- x.Round + 1

            printfn "ROUND %i" x.Round

            let proposal: ConsensusPreference =
                {
                    round = x.Round
                    preference = x.Value
                    timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                }

            let newCoordinator = x.GetCoordinator x.Round

            printfn "COORDINATOR %i" newCoordinator.port

            if newCoordinator = x.Node then
                do! x.HandlePreference proposal
                printfn "CONSENSUS SENT MESSAGE TO ITSELF %A" proposal
            else
                do! x.server.SendMessage proposal newCoordinator.host newCoordinator.port
                printfn "CONSENSUS SENT MESSAGE %A" proposal
        }

        member x.UpdateValue newValue = async {
            x.Value <- newValue
        }