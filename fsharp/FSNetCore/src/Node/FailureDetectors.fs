namespace Node

open DataTypes
open System.Collections.Generic
open NetworkServer.Communication

module FailureDetectors =

    [<AbstractClass>]
    type FailureDetector () =
        abstract member DetectFailures: Async<unit>
        abstract member ReceiveMessage: obj -> (Neighbor -> Async<unit>) -> Async<bool>
        abstract member AddNeighbor: Neighbor -> Async<unit>
        abstract member InitializeFailureDetector: NetworkServer -> HashSet<Neighbor> -> unit
        abstract member GetSuspectedList: Async<Neighbor list>
        abstract member AddSuspects: Neighbor list -> Async<unit>