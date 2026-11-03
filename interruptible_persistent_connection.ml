open! Core
open! Bonsai_web
open! Async_kernel
open! Async_rpc_kernel

(* 0.1 seconds is the default retry timeout. *)
let retry_delay = Time_ns.Span.of_sec 0.1

module Persistent_rpc_connection = Persistent_connection_kernel.Make (struct
    type t = Rpc.Connection.t

    let close t = Rpc.Connection.close t
    let is_closed t = Rpc.Connection.is_closed t
    let close_finished t = Rpc.Connection.close_finished t
  end)

type t =
  { connection : Persistent_rpc_connection.t
  ; time_source : Time_source.Read_write.t
  ; connector : Rpc_effect.Connector.t
  ; connectable : bool ref
  }

let connector { connector; _ } = connector

let kill_connection { connection; time_source = _; connector = _; connectable = _ } =
  let%bind () =
    connection
    |> Persistent_rpc_connection.current_connection
    |> Option.value_exn
    |> Rpc.Connection.close
  in
  Async_kernel_scheduler.yield ()
;;

let fail_to_connect { connection; time_source; connectable; connector = _ } =
  connectable := false;
  let conn_or_error =
    Persistent_rpc_connection.connected_or_failed_to_connect connection
  in
  let%bind () = Time_source.advance_by_alarms_by time_source retry_delay in
  match%bind conn_or_error with
  | Ok _conn -> failwith "connection attempt succeeded unexpectedly"
  | Error _ ->
    connectable := true;
    Async_kernel_scheduler.yield_until_no_jobs_remain ()
;;

let next_connection { connection; time_source; connectable; connector = _ } =
  if not !connectable then failwith "currently not connectable";
  let%bind () = Time_source.advance_by_alarms_by time_source retry_delay in
  let%bind _connection = Persistent_rpc_connection.connected connection in
  Async_kernel_scheduler.yield_until_no_jobs_remain ()
;;

let create_connection ~connection_state implementations =
  let to_server = Pipe.create () in
  let to_client = Pipe.create () in
  let one_connection implementations pipe_to pipe_from =
    let transport =
      Pipe_transport.create Pipe_transport.Kind.string (fst pipe_to) (snd pipe_from)
    in
    let%bind conn = Rpc.Connection.create ?implementations ~connection_state transport in
    return (Result.ok_exn conn)
  in
  don't_wait_for
    (let%bind server_conn =
       one_connection
         (Some
            (Rpc.Implementations.create_exn
               ~implementations
               ~on_unknown_rpc:`Continue
               ~on_exception:Log_on_background_exn))
         to_server
         to_client
     in
     Rpc.Connection.close_finished server_conn);
  let%map connection = one_connection None to_client to_server in
  Or_error.return connection
;;

let create_internal
  ?(on_conn_failure = Rpc_effect.On_conn_failure.Retry_until_success)
  ~connection_state
  ~implementations
  ()
  =
  let time_source =
    Synchronous_time_source.create ~now:Time_ns.epoch () |> Time_source.of_synchronous
  in
  let connectable = ref true in
  let connection =
    Persistent_rpc_connection.create
      ~server_name:"test_server"
      ~connect:(fun () ->
        match !connectable with
        | false -> Deferred.Or_error.error_string "Connection attempt failed"
        | true -> create_connection ~connection_state (implementations ()))
      ~address:(module Unit)
      ~random_state:`Non_random
      ~retry_delay:(fun () -> retry_delay)
      ~time_source:(Time_source.read_only time_source)
      (fun () -> Deferred.Or_error.return ())
  in
  let connector =
    Rpc_effect.Connector.persistent_connection
      ~on_conn_failure
      (module Persistent_rpc_connection)
      connection
  in
  { connection; time_source; connector; connectable }
;;

let create' ~connection_state ~implementations =
  create_internal ~connection_state ~implementations ()
;;

let create ?on_conn_failure ~connection_state implementations =
  create_internal
    ?on_conn_failure
    ~connection_state
    ~implementations:(fun () -> implementations)
    ()
;;
