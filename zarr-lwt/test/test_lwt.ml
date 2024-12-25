open OUnit2
open Zarr
open Zarr.Indexing
open Zarr.Codecs
open Zarr_lwt.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: Node.Array.t list * Node.Group.t list]

module type LWT_STORE = Zarr.Storage.S with type 'a io := 'a Lwt.t

let test_storage
  (type a) (module M : LWT_STORE with type t = a) (store : a) =
  let open M in
  let open IO.Infix in
  let gnode = Node.Group.root in

  hierarchy store >>= fun nodes ->
  assert_equal ~printer:print_node_pair ([], []) nodes;

  Group.create store gnode >>= fun () ->
  Group.exists store gnode >>= fun exists ->
  assert_equal ~printer:string_of_bool true exists;

  Group.metadata store gnode >>= fun meta ->
  assert_equal ~printer:Metadata.Group.show Metadata.Group.default meta;

  Group.delete store gnode >>= fun () ->
  Group.exists store gnode >>= fun exists ->
  assert_equal ~printer:string_of_bool false exists;
  hierarchy store >>= fun nodes ->
  assert_equal ~printer:print_node_pair ([], []) nodes;

  let attrs = `Assoc [("questions", `String "answer")] in
  Group.create ~attrs store gnode >>= fun () ->
  Group.metadata store gnode >>= fun meta ->
  assert_equal ~printer:Yojson.Safe.show attrs @@ Metadata.Group.attributes meta;

  Array.exists store @@ Node.Array.(gnode / "non-member") >>= fun exists ->
  assert_equal ~printer:string_of_bool false exists;

  let cfg =
    {chunk_shape = [2; 5; 5]
    ;index_location = End
    ;index_codecs = [`Bytes BE]
    ;codecs = [`Bytes LE]} in
  let anode = Node.Array.(gnode / "arrnode") in
  let slice = [R (0, 20); I 10; R (0, 29)] in
  let exp = Ndarray.init Ndarray.Complex32 [21; 1; 30] (Fun.const Complex.one) in

  Lwt_list.iter_s
    (fun codecs ->
      Array.create
        ~codecs ~shape:[100; 100; 50] ~chunks:[10; 15; 20]
        Ndarray.Complex32 Complex.one anode store >>= fun () ->
      Array.write store anode slice exp >>= fun () ->
      Array.read store anode slice Complex32 >>= fun got ->
      assert_equal exp got;
      Ndarray.fill exp Complex.{re=2.0; im=0.};
      Array.write store anode slice exp >>= fun () ->
      Array.read store anode slice Complex32 >>= fun arr ->
      assert_equal exp arr;
      Ndarray.fill exp Complex.{re=0.; im=3.0};
      Array.write store anode slice exp >>= fun () ->
      Array.read store anode slice Complex32 >>= fun got ->
      assert_equal exp got;
      match codecs with
      | [`ShardingIndexed _] -> Array.delete store anode
      | _ -> IO.return_unit)
    [[`ShardingIndexed cfg]; [`Bytes BE]] >>= fun () ->

  let child = Node.Group.of_path "/some/child/group" in
  Group.create store child >>= fun () ->
  Group.children store gnode >>= fun (arrays, groups) ->
  assert_equal
    ~printer:string_of_list ["/arrnode"] (List.map Node.Array.to_path arrays);
  assert_equal
    ~printer:string_of_list ["/some"] (List.map Node.Group.to_path groups);

  Group.children store @@ Node.Group.(root / "fakegroup") >>= fun c ->
  assert_equal ([], []) c;

  hierarchy store >>= fun (ac, gc) ->
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/arrnode"; "/some"; "/some/child"; "/some/child/group"] got;

  (* tests for renaming nodes *)
  let some = Node.Group.of_path "/some/child" in
  Array.rename store anode "ARRAYNODE" >>= fun () ->
  Group.rename store some "CHILD" >>= fun () ->
  hierarchy store >>= fun (ac, gc) ->
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/ARRAYNODE"; "/some"; "/some/CHILD"; "/some/CHILD/group"] got;
  (* restore old array node name. *)
  Array.rename store (Node.Array.of_path "/ARRAYNODE") "arrnode" >>= fun () ->

  let nshape = [25; 32; 10] in
  Array.reshape store anode nshape >>= fun () ->
  Array.metadata store anode >>= fun meta ->
  assert_equal ~printer:[%show : int list] nshape @@ Metadata.Array.shape meta;

  Array.delete store anode >>= fun () ->
  clear store >>= fun () ->
  hierarchy store >>= fun got ->
  assert_equal ~printer:print_node_pair ([], []) got;
  IO.return_unit

module type SYNC_PARTIAL_STORE = sig
  exception Not_implemented
  include Zarr.Storage.S with type 'a io := 'a Lwt.t
end

let test_readable_writable_only 
  (type a) (module M : SYNC_PARTIAL_STORE with type t = a) (store : a) =
  let open M in
  let open IO.Syntax in
  let assert_not_implemented f =
    Lwt.catch
      (fun () -> let* _ = f () in IO.return_unit)
      (function
        | Not_implemented -> IO.return_unit
        | _ -> failwith "Supposed to raise Not_implemented")
  in
  let gnode = Node.Group.root in
  let attrs = `Assoc [("questions", `String "answer")] in
  let* () = Group.create ~attrs store gnode in
  let* exists = Group.exists store gnode in
  assert_equal ~printer:string_of_bool true exists;
  let* meta = Group.metadata store gnode in
  assert_equal ~printer:Yojson.Safe.show attrs (Metadata.Group.attributes meta);
  let* exists = Array.exists store Node.Array.(gnode / "non-member") in
  assert_equal ~printer:string_of_bool false exists;
  let cfg =
    {chunk_shape = [|2; 5; 5|]
    ;index_location = End
    ;index_codecs = [`Bytes LE]
    ;codecs = [`Transpose [|2; 0; 1|]; `Bytes BE]} in
  let anode = Node.Array.(gnode / "arrnode")
  and slice = [|R [|0; 5|]; I 10; R [|0; 10|]|]
  and bigger_slice = [|R [|0; 6|]; L [|9; 10|] ; R [|0; 11|]|]
  and codecs = [`ShardingIndexed cfg] and shape = [|100; 100; 50|] and chunks = [|10; 15; 20|] in
  let* () = Array.create ~codecs ~shape ~chunks Complex32 Complex.one anode store in
  let exp = Ndarray.init Complex32 [|6; 1; 11|] (Fun.const Complex.one) in
  let* got = Array.read store anode slice Complex32 in
  assert_equal exp got;
  Ndarray.fill exp Complex.{re=2.0; im=0.};
  let* () = Array.write store anode slice exp in
  let* got = Array.read store anode slice Complex32 in
  (* test if a bigger slice containing new elements can be read from store *)
  let* _ = Array.read store anode bigger_slice Complex32 in
  assert_equal exp got;
  (* test writing a bigger slice to store *)
  let* () = Array.write store anode bigger_slice @@ Ndarray.init Complex32 [|7; 2; 12|] (Fun.const Complex.{re=0.; im=3.0}) in
  let* got = Array.read store anode slice Complex32 in
  Ndarray.fill exp Complex.{re=0.; im=3.0};
  assert_equal exp got;
  let nshape = [|25; 28; 10|] in
  let* () = Array.reshape store anode nshape in
  let* meta = Array.metadata store anode in
  assert_equal ~printer:print_int_array nshape (Metadata.Array.shape meta);
  let* () = assert_not_implemented (fun () -> Array.rename store anode "newname") in
  let* () = assert_not_implemented (fun () -> Group.children store gnode) in
  let* () = assert_not_implemented (fun () -> hierarchy store) in
  let* () = assert_not_implemented (fun () -> Group.delete store gnode) in
  let* () = assert_not_implemented (fun () -> clear store) in
  IO.return_unit

module Dir_http_server = struct
  module S = Tiny_httpd

  let make ~max_connections ~dir () =
    let server = S.create ~max_connections ~addr:"127.0.0.1" ~port:8080 () in
    (* HEAD request handler *)
    S.add_route_handler server ~meth:`HEAD S.Route.rest_of_path_urlencoded (fun path _ ->
      let fspath = Filename.concat dir path in
      match In_channel.(with_open_gen [Open_rdonly] 0o700 fspath length) with
      | exception Sys_error e -> S.Response.make_raw ~code:404 e
      | l ->
        let headers =
          [("Content-Length", Int64.to_string l)
          ;("Content-Type",
            if String.ends_with ~suffix:".json" path
            then "application/json"
            else "application/octet-stream")]
        in
        let r = S.Response.make_raw ~code:200 "" in
        S.Response.update_headers (List.append headers) r
    );
    (* GET request handler *)
    S.add_route_handler server ~meth:`GET S.Route.rest_of_path_urlencoded (fun path _ ->
      let fspath = Filename.concat dir path in
      match In_channel.(with_open_gen [Open_rdonly] 0o700 fspath input_all) with
      | exception Sys_error _ -> S.Response.make_raw ~code:404 (Printf.sprintf "%s not found" path)
      | s ->
        let headers =
          [("Content-Length", Int.to_string (String.length s))
          ;("Content-Type",
            if String.ends_with ~suffix:".json" path
            then "application/json"
            else "application/octet-stream")]
        in
        S.Response.make_raw ~headers ~code:200 s
    );
    (* POST request handler *)
    S.add_route_handler_stream server ~meth:`POST S.Route.rest_of_path_urlencoded (fun path req ->
      let write oc = 
        let max_size = 1024 * 10 * 1024 in
        let req' = S.Request.limit_body_size ~bytes:(Bytes.create 4096) ~max_size req in
        S.IO.Input.iter (Out_channel.output oc) req'.body;
        Out_channel.flush oc
      in
      let fspath = Filename.concat dir path in
      Zarr.Util.create_parent_dir fspath 0o700;
      let f = [Open_wronly; Open_trunc; Open_creat] in
      match Out_channel.(with_open_gen f 0o700 fspath write) with
      | exception Sys_error e -> S.Response.make_raw ~code:500 e
      | () ->
        let opt = List.assoc_opt "content-type" req.headers in
        let content_type = Option.fold ~none:"application/octet-stream" ~some:Fun.id opt in
        let headers = [("content-type", content_type); ("Connection", "close")] in
        S.Response.make_raw ~headers ~code:201 (Printf.sprintf "%s created" path)
    );
    (* DELETE request handler *)
    S.add_route_handler server ~meth:`DELETE S.Route.rest_of_path_urlencoded (fun path _ ->
      let fspath = Filename.concat dir path in
      match Sys.remove fspath with
      | exception Sys_error e -> S.Response.make_raw ~code:404 e
      | () ->
        let headers = [("Connection", "close")] in
        S.Response.make_raw ~headers ~code:200 (Printf.sprintf "%s deleted successfully" path)
    );
    server

  let run_with t after_init =
    let perform () =
      let _ = Thread.create S.run_exn t in
      Lwt.dont_wait after_init raise;
      IO.return_unit
    in
    Fun.protect ~finally:(fun () -> S.stop t) perform
end

let _ = 
  run_test_tt_main @@ ("Run Zarr Lwt API tests" >::: [
    "test lwt-based stores" >::
    (fun _ ->
      let rand_num = string_of_int @@ Random.int 100_000 in
      let tmp_dir = Filename.(concat (get_temp_dir_name ()) (rand_num ^ ".zarr")) in
      let s = FilesystemStore.create tmp_dir in

      assert_raises
        (Sys_error (Format.sprintf "%s: File exists" tmp_dir))
        (fun () -> FilesystemStore.create tmp_dir);

      (* ensure it works with an extra "/" appended to directory name. *)
      ignore @@ FilesystemStore.open_store (tmp_dir ^ "/");

      let fakedir = "non-existant-zarr-store112345.zarr" in
      assert_raises
        (Sys_error (Printf.sprintf "%s: No such file or directory" fakedir))
        (fun () -> FilesystemStore.open_store fakedir);

      let fn = Filename.temp_file "nonexistantfile" ".zarr" in
      assert_raises
        (Zarr.Storage.Not_a_filesystem_store fn)
        (fun () -> FilesystemStore.open_store fn);

      (* ZipStore configuration *)
      let zpath = tmp_dir ^ ".zip" in
      (* AmazonS3Store configuration *)
      let region = Aws_s3.Region.minio ~port:9000 ~host:"localhost" ()
      and bucket = "test-bucket-lwt"
      and profile = "default" in

      let promises = 
        [ZipStore.with_open `Read_write zpath (test_storage (module ZipStore))
         (* test just opening the now exisitant archive created by the previous test. *)
        ;ZipStore.with_open `Read_only zpath (fun _ -> Lwt.return_unit)
        ;AmazonS3Store.with_open ~region ~bucket ~profile (test_storage (module AmazonS3Store))
        ;test_storage (module MemoryStore) (MemoryStore.create ())
        ;test_storage (module FilesystemStore) s
        ;begin
          let server = Dir_http_server.make ~max_connections:1 ~dir:tmp_dir () in
          Dir_http_server.run_with server @@ fun () ->
          HttpStore.with_open "127.0.0.1:8080" (test_readable_writable_only (module HttpStore))
        end
        ]
      in
      Lwt_main.run @@ Lwt.join promises)
])
