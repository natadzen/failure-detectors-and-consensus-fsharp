namespace NetworkServer

module Communication =
    open System
    open System.Net
    open System.Net.Sockets

    type ServerEndpoint = { Ip: IPAddress; Port: int }
    type NodeId = string * ServerEndpoint

    [<AbstractClass>]
    type NetworkServer () =
        abstract member StartServer: Async<unit>
        abstract member SendMessage: obj -> string -> int -> Async<unit>
        abstract member ReceiveMessages: Async<unit>

    type TcpServer(port:int, processMessage) =
        inherit NetworkServer()

        override x.SendMessage (message: obj) (toHost: string) (toPort: int) = async {
            let! messageBytes = MessageSerialization.serializeNewtonsoft message

            use client = new TcpClient()
            client.Connect(IPAddress.Parse(toHost), toPort)
            use stream = client.GetStream()
            let size = messageBytes.Length
            let sizeBytes = BitConverter.GetBytes size
            do! stream.AsyncWrite(sizeBytes, 0, sizeBytes.Length)
            do! stream.AsyncWrite(messageBytes, 0, messageBytes.Length)
        }

        override x.ReceiveMessages = async {
            printfn "Listening for incoming TCP messages..."

            let listener = TcpListener(IPAddress.Loopback, port)
            listener.Start()

            while true do
                let client = listener.AcceptTcpClient()
                try
                    let stream = client.GetStream()
                    let sizeBytes = Array.create 4 0uy
                    let! readSize = stream.AsyncRead(sizeBytes, 0, 4)
                    let size = BitConverter.ToInt32(sizeBytes, 0)
                    let messageBytes = Array.create size 0uy
                    let! bytesReceived = stream.AsyncRead(messageBytes, 0, size)
                    if bytesReceived <> 0 then
                        // Process message bytes using custom logic
                        do! processMessage messageBytes
                with
                | ex ->
                    printfn "Exception receiving a TCP message: %s." ex.Message
        }

        override x.StartServer = async {
            printfn "Started a server on port %A." port
            do! x.ReceiveMessages
        }


    type UdpServer (port:int, processMessage) =
        inherit NetworkServer()

        override x.SendMessage (message: obj) (toHost: string) (toPort: int) =
            async {
                try
                    let! messageBytes = MessageSerialization.serializeNewtonsoft message

                    let udpClient = new UdpClient()
                    udpClient.Connect(toHost, toPort)

                    udpClient.Send(messageBytes, messageBytes.Length) |> ignore
                    udpClient.Close()
                with
                | ex ->
                    printf "Exception sending a UDP message: %s." ex.Message
            }

        override x.ReceiveMessages = async {
            printfn "Listening for incoming UDP messages..."
            let udpClient = new UdpClient(port)

            let receive =
                async {
                    try
                        let remoteNode = IPEndPoint(IPAddress.Any, 0)
                        let messageBytes = udpClient.Receive(ref remoteNode)

                        // Process message bytes using custom logic
                        do! processMessage messageBytes
                    with
                    | ex ->
                        printf "Exception receiving a UDP message: %s." ex.Message
                }

            while true do
                do! receive
        }

        override x.StartServer = async {
            printfn "Started a server on port %A." port
            do! x.ReceiveMessages
        }
