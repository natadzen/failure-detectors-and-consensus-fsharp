// Learn more about F# at http://fsharp.org

open Node
open Node.DataTypes
open System.Threading

let messageHandling message = async {
    printfn "User message handling here..."
}

let startConsensus (node: Node) = async {
    // Start consensus after 10 seconds
    do! Async.Sleep 10000
    do! node.StartConsensus ()
}

let getHostAndPort (address: string) =
    let hostport = address.Split([|':'|])
    { host = hostport.[0]; port = int hostport.[1] }

[<EntryPoint>]
let main argv =
    printfn "Hello from the F# Distributed System!"

    let printUsage () =
        printfn "USAGE:"
        printfn "First argument is host and port to run this node on."
        printfn "Second argument is a comma separated list of hosts and ports of the neighbors."
        printfn "Third argument is a default value to start the node with."
        printfn """Example: dotnet run "127.0.0.1:1234" "127.0.0.1:1235,127.0.0.1:1236" "Value" """
    if argv.Length = 0 then
        printUsage ()
    else
        if argv.Length < 1 || argv.Length > 3 then
            printUsage()
        else
            let nodeInput = Array.get argv 0
            let node = getHostAndPort nodeInput

            let startingValueConf, neighborsConf =
                match argv.Length with
                | 1 -> ("Default", [])
                | 2 ->
                    let neighborsInput = Array.get argv 1
                    let neighbors = neighborsInput.Split([|','|]) |> Seq.map getHostAndPort |> Seq.toList
                    ("Default", neighbors)
                | 3 ->
                    let neighborsInput = Array.get argv 1
                    let neighbors = neighborsInput.Split([|','|]) |> Seq.map getHostAndPort |> Seq.toList
                    (Array.get argv 2, neighbors)
                | _ -> failwith "This shouldn't ever happen."

            printfn "Starting Node %s:%i..." node.host node.port

            let nodeInstance = Node.Node(node.host, node.port, startingValueConf)
            let nodeConfiguration = {
                networkProtocol = UDP
                failureDetector = HeartbeatSuspectLevel
                consensus = ChandraToueg
                receiveMessageFunction = messageHandling
                neighbors = neighborsConf
                gossipping = true
                verbose = false
            }

            nodeInstance.InitializeNode(nodeConfiguration) |> Async.RunSynchronously
            nodeInstance.Start |> Async.RunSynchronously

            // Uncomment to experiment with consensus
            nodeInstance |> startConsensus |> Async.RunSynchronously

            Thread.Sleep 600000

    0 // return an integer exit code
