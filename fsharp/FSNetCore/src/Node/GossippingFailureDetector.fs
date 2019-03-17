namespace Node

open DataTypes
open System.Collections.Generic
open NetworkServer.Communication
open FailureDetectors

module GossippingFailureDetector =

    type SendSuspectedList = {
        suspectedList: Neighbor list
    }

    type GossippingFailureDetector (failureDetector) =
        inherit FailureDetector ()

        [<DefaultValue>] val mutable innerFailureDetector : FailureDetector
        [<DefaultValue>] val mutable server : NetworkServer
        [<DefaultValue>] val mutable neighbors : HashSet<Neighbor>

        member val gossipInterval = 10000

        override x.InitializeFailureDetector (server: NetworkServer) (neighbors: HashSet<Neighbor>) =
            x.innerFailureDetector <- failureDetector
            x.server <- server
            x.neighbors <- neighbors
            x.innerFailureDetector.InitializeFailureDetector server neighbors

        override x.DetectFailures = async {
            do! x.innerFailureDetector.DetectFailures |> Async.StartChild |> Async.Ignore

            do! x.GossippingSuspects |> Async.StartChild |> Async.Ignore
        }

        override x.ReceiveMessage message updateNeighborsFunction = async {
            let! messageReceived = x.innerFailureDetector.ReceiveMessage message updateNeighborsFunction
            if messageReceived then return true
            else
                match message with
                | :? SendSuspectedList as suspectList ->
                    do! x.HandleReceivedSuspectList suspectList
                    return true
                | _ ->
                    return false
        }

        override x.AddNeighbor neighbor = async {
            do! x.innerFailureDetector.AddNeighbor neighbor
        }

        override x.AddSuspects neighbors = async {
            do! x.innerFailureDetector.AddSuspects neighbors
        }

        override x.GetSuspectedList = async {
            return! x.innerFailureDetector.GetSuspectedList
        }

        member x.GossippingSuspects = async {
            printfn "Set up gossipping susepcts schedule."
            try
                while true do
                    // Waiting a number of milliseconds before gossipping again
                    do! Async.Sleep x.gossipInterval

                    let! suspectedList = x.GetSuspectedList

                    if not (suspectedList |> List.isEmpty) then
                        printfn "Sending Suspect List: %A" suspectedList

                        // Communicating suspects to neighbors
                        for n in x.neighbors do
                            do! x.SendSuspects n suspectedList
            with
            | ex->
                printfn "Gossipping Suspects Workflow Exception: %s" ex.Message
        }

        member x.SendSuspects neighbor suspectedList = async {
            do! x.server.SendMessage { suspectedList = suspectedList } neighbor.host neighbor.port
        }

        member x.HandleReceivedSuspectList suspectList = async {
            printfn "Received Suspect List: %A" suspectList.suspectedList
            do! x.AddSuspects suspectList.suspectedList
        }