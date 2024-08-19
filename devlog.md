PinBoard:
    . Implement the Write-ahead-log, which can be replayed or appended, the value type need to be user defined(the new message type).
    . Segmented log, async file IO, low water mark.
    . Implement recover logic.

15/Aug/2024 afternoon
Complete the signature part.
I may need to build another connection part which is a synchronous version.

13/Aug/2024 afternoon
I used the ecdsa signature to verify the message and implement the whole logic.
Planned to encrypt the data using AES, in fact , I want to refactor the whole messages to encrypted in AES, but the amount of work would be so huge.

10/Aug/2024 evening
    Going to ECDSA's wiki page https://en.wikipedia.org/wiki/Elliptic_Curve_Digital_Signature_Algorithm, I got a brief understanding of how ECDSA works.
    I found that in openssl crate, there are a lot of encrtption algorithm including Rsa and ECDSA. So I will transfer the Rsa encryption using rsa crate to that in openssl for simplicity.

05/Aug/2024 night
Completed:
    Implemented the full logic of Rsa commmunication and handle it like a userdefined message.

04/Aug/2024 night
Completed:
    Implemented the short connection version and tested.
    Completed a big part of rsa signature: logic of getting other node's public key.
To be Completed:
    The full logic of using private key and public key to sending encrypted infomation to each other.


29/Jul/2024 night
Seems that there are something wrong in the conn_pool, I still can't implement the long connection.

28/Jul/2024 afternoon
I found that there is some problems in the former method of dealing with the user defined message. Because the handle_connection is called by the start_listening method, so it's return value can't be used by the user. It still needs handler registration to accomplish this. I have completed a draft, hope it will work, I'll test it tonight.

27/Jul/2024 afternoon
Complete:
    Support for user defined message. Including write and read.
    Maintain a conn_pool to ease the message burden.

20/Jul/2024 afternoon
Comeplete:
    Public API for user to send out message of their own.

16/Jul/2024 night
These 2 days I reviewed the code of salticidae and find that salticidae use a event loop for user defined logic.
So I tried to build my own, I haven't tested it yet and it needs so deep knowledge to fully understand it, which I would try next few days.
Completed:
    AsyncEventQueue draft.

14/Jul/2024 afternoon
Completed:
    completed the rest of segment struct

13/Jul/2024 night
Completed:
    completed the create and open function of segment struct.

12/Jul/2024 night
Completed:
    Ported the the memory map part and some of the segment part.
    Initiated the Serde serailized version of message, which could save a lot of time for parsing the message.

11/Jul/2024 night
Begin to implement write ahead log, the effort and amount of code beyond my expectation. Use unsafe rust, memory reflection and segmented log.
Completed:
    A simple wal.

11/Jul/2024 afternoon
Completed:
    Implemented the error log.

10/Jul/2024 night
Completed:
    Implement the logic of initiate a node with argument. Then I can indicate the id and address with run the node.

09/Jul/2024 night
Completed:
    Implemented the recovery snapshot and stored it through file IO.
    Implemented the serialization and deserialization of Backup.
    Implemented backup mechanism.
    Optimize the overall structure a little.

08/Jul/2024 night
Completed:
    Implmented the 'establish' message and the handle part. This message is mainly for recovering nodes to use.

08/Jul/2024 night
Removed all the locks and use Dashmap which is thread safe.
Removed connection pool, it is Tcp long connection, there are a lot of stuff there.
Every thing is fine now.

07/Jul/2024 night
Change the code throughly.

07/Jul/2024 evening
Completed:
    Tried to use docker to hold client and server, succeed ugly.
    Optimize code to use log to handle some of the error that can be tolerated, such as send hb.
    Optimize code to use some Rust chained code on iterators.

06/Jul/2024 evening
Completed:
    Message type 'esatblish'.
    Opetimize the code to avoid dead lock.
    A method for establishing a connection between two nodes.
    Optimize the logic of connection, the heartbeat broadcasting operation no longer establish connections to new or dropped nodes.
    Add some comment.

05/Jul/2024 night
Completed:
    Add some comments.
    Add signature to heartbeat message and add verification in parsing heartbeart.

05/Jul/2024 afternoon
Completed:
    Use a Tcp connection pool to maintain Tcp connection for those haven't been dropped. Increased performance of asynchronous IO.

03/Jul/2024 night
Completed:
    Add something about crypto, but I don't understand it.

03/Jun/2024 evening
node2.rs use Rwlock and dashmap to optimize the performance.
But I don't know about Rwlock and dashmap, so I will check them.

03/Jul/2024 noon
Completed:
    The implementation of a node.
    The node asynchronously handle connections, send heartbeats and check neighbors' activity.
Question:
    How can a node get a neighbor's address.

02/Jul/2024 night
Completed:
    HeartBeat of node.

30/Jun/2024 night
Completed:
    'apply' method can just apply for specific 4 message types, others need to do there own job.
    There is a state I have to implement, but I don't know how to decide which field need to be thread-safe which needn't to be.

30/Jun/2024 afternoon
Completed:
    something about config and heartbeat.

28/Jun/2024 evening
Completed:
    A simple demo of ping-pong. (Just cargo run --bin server and client)

27/Jun/2024 night(2)
Completed:
    The Set message.
    The Unknown message.

27/Jun/2024 night
Completed:
    The Ping message, looks like I don't have to implement heartbeat because it is identical with Ping.

27/Jun/2024 evening
Several things to say:
    1. The 'Frame' is actually RESP(Redis serialization protocol), a fast and easy to implement serialization protocol.
    2. The message type need to be extended later, carrying sender's infomation.
    3. The 'apply' method of each message type need to be extended when operations need.
    4. Need to implement snapshot, using serde. And maybe referencing to RDB and AOF of redis can help.
    5. Need to use snapshot as crash-recovery methods.

27/Jun/2024 afternoon
Completed:
    Implementation of Get message, notice that this is different from Get command in redis because this key is u64 and value is String while in redis is String and bytes.
To be completed:
    other message type.
    server and client.

26/Jun/2024 evening
Completed:
    The skeleton of msg.rs, there could be message type in the future.
    By the way, I need to think about the operation by the node self, for example propose in Paxos.
To be Completed:
    I need to read the raft, viewstamp and pbft paper, to get the operation they use. There must be more in the Db and Message.
    And the specific message operation need to be implemented.

25/Jun/2024 night
Completed:
    Completed the db.rs, think a lot about my project:
    I can use frameworks like serde, tonic, etc. But those frameworks are too big
    and I don't need that many features because I just need a little of them.
    At first I failed to build a demo on my conn and frame mod. Then I began to use tokio_util
    to complete this. Later I found that there was a mistake on my implemtation of redis protocol.
    After fixed that, things worked. I can use those frames to build various command and messages.
To be completed:
    heartbeat, broadcast and snapshoot.
    and I need to implement backup when needed.

25/Jun/2024 afternoon
Completed:
    Do something on the Db. The Db will be responsible for use for handle data
    which maybe visited repeatly. Such as log and neighbor nodes.
To be completed:
    I need to implement a short version of ping-pong.

24/Jun/2024 night
Completed:
    added some comments for conn.rs and frame.rs.
   complete the frame.rs.
   start a db.rs.
To be completed:
   I need to use a db intance for the library because asynchronous operations could lead to data race.

23/Jun/2024 night
Completed:
    next_bytes, next_string, next_int, parse.rs.
To be completed:
  Feaguring out my own cmd list.
  After some thinking, I need parser libraries to translate dsl to rust
  code.
  I need to implement neighbor and broadcast first.

23/Jun/2024 morning
Completed:
    struct Parse, enum ParseError, From<String> for ParseError, impl new and next for Parse.
    Feagured out what the meaning of cmd and parse.
To be completed:
    The remainning of parse.rs, and feaguring out my own cmd list.

20/Jun/2024 night
Completed:
    get_decimal, get_line, from, try_into, impl others
To be completed:
    implementing ping pong between 2 nodes.

18/Jun/2024 1am
Completed:
    parse,
To be completed:
    to_vec, into, advance, get_line, get_decimal, try_into

17/Jun/2024 afternoon
Completed:
    Feagured out what cursor for.
    write_decimal, skip, peek_u8, get_u8
To be completed:
    parse, advance, into, get_line, get_decimal, try_into.

16/Jun/2024 night
Completed:
    Feagured out what frame is.
    write_frame, write_value, check.
To be completed:
    write_decimal, parse, advance, into, get_u8, get_line, get_decimal, skip, peek_u8, try_into.

16/Jun/2024
1. Frame, Frame::check, Frame::parse.
2. Result.
3. buffer::advance.
4. Any way, check the source code of mini-redis and implement the same way.

15/Jun/2024
1. This could be a library crate for use later, but now I need to implement the Paxos example.
2. Value transferred by network should be seriallized. But now just use u32 to keep simple.
3. Connection and messages sending need to be asynchronously implememted by Tokio.
4. I need several other instances using this code to be proposer, acceptor and learner. I am not sure if I can place tham in a bin folder like in tokio tutorial while keeping use the lib code.
