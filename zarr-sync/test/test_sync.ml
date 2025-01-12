open OUnit2
open Zarr
open Zarr.Indexing
open Zarr.Codecs
open Zarr_sync.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: Node.Array.t list * Node.Group.t list]

module type SYNC_STORE = Zarr.Storage.S with type 'a io := 'a

let test_storage
  (type a) (module M : SYNC_STORE with type t = a) (store : a) =
  let open M in
  let gnode = Node.Group.root in

  let nodes = hierarchy store in
  assert_equal ~printer:print_node_pair ([], []) nodes;

  Group.create store gnode;
  let exists = Group.exists store gnode in
  assert_equal ~printer:string_of_bool true exists;
  assert_equal ~printer:print_node_pair ([], [gnode]) (hierarchy store);

  let meta = Group.metadata store gnode in
  assert_equal ~printer:Metadata.Group.show Metadata.Group.default meta;

  Group.delete store gnode;
  let exists = Group.exists store gnode in
  assert_equal ~printer:string_of_bool false exists;
  let nodes = hierarchy store in
  assert_equal ~printer:print_node_pair ([], []) nodes;

  let attrs = `Assoc [("questions", `String "answer")] in
  Group.create ~attrs store gnode;
  let meta = Group.metadata store gnode in
  assert_equal ~printer:Yojson.Safe.show attrs @@ Metadata.Group.attributes meta;

  let exists = Array.exists store @@ Node.Array.(gnode / "non-member") in
  assert_equal ~printer:string_of_bool false exists;

  let cfg =
    {chunk_shape = [2; 5; 5]
    ;index_location = End
    ;index_codecs = [`Bytes LE; `Crc32c]
    ;codecs = [`Transpose [2; 0; 1]; `Bytes BE; `Zstd (0, false)]} in
  let cfg2 =
    {chunk_shape = [2; 5; 5]
    ;index_location = Start
    ;index_codecs = [`Bytes BE]
    ;codecs = [`Bytes LE]} in
  let anode = Node.Array.(gnode / "arrnode") in
  let slice = [R (0, 20); I 10; R (0, 29)] in
  let bigger_slice =  [R (0, 21); L [9; 10] ; R (0, 30)] in

  List.iter
    (fun codecs ->
      Array.create ~codecs ~shape:[100; 100; 50] ~chunks:[10; 15; 20] Complex32 Complex.one anode store;
      let exp = Ndarray.init Complex32 [21; 1; 30] (Fun.const Complex.one) in
      let got = Array.read store anode slice Complex32 in
      assert_equal exp got;
      Ndarray.fill exp Complex.{re=2.0; im=0.};
      Array.write store anode slice exp;
      let got = Array.read store anode slice Complex32 in
      (* test if a bigger slice containing new elements can be read from store *)
      let _ = Array.read store anode bigger_slice Complex32 in
      assert_equal exp got;
      (* test writing a bigger slice to store *)
      Array.write store anode bigger_slice @@ Ndarray.init Complex32 [22; 2; 31] (Fun.const Complex.{re=0.; im=3.0});
      let got = Array.read store anode slice Complex32 in
      Ndarray.fill exp Complex.{re=0.; im=3.0};
      assert_equal exp got;
      Array.delete store anode)
    [[`ShardingIndexed cfg]; [`ShardingIndexed cfg2]];

  (* repeat tests for non-sharding codec chain *)
  Array.create ~sep:`Dot ~codecs:[`Bytes BE] ~shape:[100; 100; 50] ~chunks:[10; 15; 20] Ndarray.Int Int.max_int anode store;
  assert_equal ~printer:print_node_pair ([anode], [gnode]) (hierarchy store);
  (* test path where there is no chunk key present in store *)
  let exp = Ndarray.init Int [21; 1; 30] (Fun.const Int.max_int) in
  Array.write store anode slice exp;
  let got = Array.read store anode slice Int in
  assert_equal exp got;
  (* test path where there is a chunk key present in store at write time. *)
  Array.write store anode slice exp;
  let got = Array.read store anode slice Int in
  assert_equal exp got;

  assert_raises
    (Zarr.Storage.Invalid_data_type)
    (fun () -> Array.read store anode slice Ndarray.Char);
  let badslice = [R (0, 20); I 10; F; F] in
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> Array.read store anode badslice Ndarray.Int);
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> Array.write store anode badslice exp);
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> Array.write store anode [R (0, 20); F; F] exp);
  let badarray = Ndarray.init Float64 [21; 1; 30] (Fun.const 0.) in
  assert_raises
    (Zarr.Storage.Invalid_data_type)
    (fun () -> Array.write store anode slice badarray);

  let child = Node.Group.of_path "/some/child/group" in
  Group.create store child;
  let arrays, groups = Group.children store gnode in
  assert_equal
    ~printer:string_of_list ["/arrnode"] (List.map Node.Array.to_path arrays);
  assert_equal
    ~printer:string_of_list ["/some"] (List.map Node.Group.to_path groups);

  assert_equal ([], []) @@ Group.children store child;
  assert_equal ([], []) @@ Group.children store Node.Group.(root / "fakegroup");

  let ac, gc = hierarchy store in
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/arrnode"; "/some"; "/some/child"; "/some/child/group"] got;

  (* tests for renaming nodes *)
  let some = Node.Group.of_path "/some/child" in
  Group.rename store some "CHILD";
  Array.rename store anode "ARRAYNODE";
  let ac, gc = hierarchy store in
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/ARRAYNODE"; "/some"; "/some/CHILD"; "/some/CHILD/group"] got;
  assert_raises
    (Zarr.Storage.Key_not_found "fakegroup")
    (fun () -> Group.rename store Node.Group.(gnode / "fakegroup") "somename");
  assert_raises
    (Zarr.Storage.Key_not_found "fakearray")
    (fun () -> Array.rename store Node.Array.(gnode / "fakearray") "somename");

  (* restore old array node name. *)
  Array.rename store (Node.Array.of_path "/ARRAYNODE") "arrnode";
  let nshape = [25; 32; 10] in
  Array.reshape store anode nshape;
  let meta = Array.metadata store anode in
  assert_equal ~printer:[%show : int list] nshape @@ Metadata.Array.shape meta;
  assert_raises
    (Zarr.Storage.Invalid_resize_shape)
    (fun () -> Array.reshape store anode [25; 10]);
  assert_raises
    (Zarr.Storage.Key_not_found "fakegroup/zarr.json")
    (fun () -> Array.metadata store Node.Array.(gnode / "fakegroup"));

  Array.delete store anode;
  clear store;
  let got = hierarchy store in
  assert_equal ~printer:print_node_pair ([], []) got

module type SYNC_PARTIAL_STORE = sig
  exception Not_implemented
  include Zarr.Storage.S with type 'a io := 'a
end

let test_readable_writable_only 
  (type a) (module M : SYNC_PARTIAL_STORE with type t = a) (store : a) =
  let open M in
  let gnode = Node.Group.root in
  let attrs = `Assoc [("questions", `String "answer")] in
  Group.create ~attrs store gnode;
  let exists = Group.exists store gnode in
  let meta = Group.metadata store gnode in
  assert_equal ~printer:string_of_bool true exists;
  assert_equal ~printer:Yojson.Safe.show attrs (Metadata.Group.attributes meta);
  let exists = Array.exists store Node.Array.(gnode / "non-member") in
  assert_equal ~printer:string_of_bool false exists;

  let cfg =
    {chunk_shape = [2; 5; 5]
    ;index_location = End
    ;index_codecs = [`Bytes LE]
    ;codecs = [`Transpose [2; 0; 1]; `Bytes BE]} in
    let anode = Node.Array.(gnode / "arrnode") in
    let slice = [R (0, 5); I 10; R (0, 10)] in
    let bigger_slice =  [R (0, 6); L [9; 10] ; R (0, 11)] in
  Array.create
    ~codecs:[`ShardingIndexed cfg] ~shape:[100; 100; 50] ~chunks:[10; 15; 20]
    Complex32 Complex.one anode store;
  let exp = Ndarray.init Complex32 [6; 1; 11] (Fun.const Complex.one) in
  let got = Array.read store anode slice Complex32 in
  assert_equal exp got;
  Ndarray.fill exp Complex.{re=2.0; im=0.};
  Array.write store anode slice exp;
  let got = Array.read store anode slice Complex32 in
  (* test if a bigger slice containing new elements can be read from store *)
  let _ = Array.read store anode bigger_slice Complex32 in
  assert_equal exp got;
  (* test writing a bigger slice to store *)
  Array.write store anode bigger_slice @@ Ndarray.init Complex32 [7; 2; 12] (Fun.const Complex.{re=0.; im=3.0});
  let got = Array.read store anode slice Complex32 in
  Ndarray.fill exp Complex.{re=0.; im=3.0};
  assert_equal exp got;
  let nshape = [25; 28; 10] in
  Array.reshape store anode nshape;
  let meta = Array.metadata store anode in
  assert_equal ~printer:[%show: int list] nshape (Metadata.Array.shape meta);
  assert_raises
    (Zarr.Storage.Invalid_resize_shape)
    (fun () -> Array.reshape store anode [25; 10]);
  assert_raises Not_implemented (fun () -> Array.rename store anode "newname");
  assert_raises Not_implemented (fun () -> Group.children store gnode);
  assert_raises Not_implemented (fun () -> hierarchy store);
  assert_raises Not_implemented (fun () -> Group.delete store gnode);
  assert_raises Not_implemented (fun () -> clear store)

module Dir_http_server = struct
  module S = Tiny_httpd

  let make ~max_connections ~dir () =
    let server = S.create ~max_connections ~addr:"127.0.0.1" ~port:8080 () in
    (* HEAD request handler *)
    S.add_route_handler server ~meth:`HEAD S.Route.rest_of_path_urlencoded (fun path _ ->
      let headers = [("Content-Type", if String.ends_with ~suffix:".json" path then "application/json" else "application/octet-stream")] in
      let fspath = Filename.concat dir path in
      let headers = match In_channel.(with_open_gen [Open_rdonly] 0o700 fspath length) with
        | exception Sys_error _ -> ("Content-Length", "0") :: headers
        | l -> ("Content-Length", Int64.to_string l) :: headers
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
    S.add_route_handler server ~meth:`POST S.Route.rest_of_path_urlencoded (fun path req ->
      let write oc = Out_channel.(output_string oc req.S.Request.body; flush oc) in
      let fspath = Filename.concat dir path in
      Zarr.Util.create_parent_dir fspath 0o700;
      let f = [Open_wronly; Open_trunc; Open_creat] in
      match Out_channel.(with_open_gen f 0o700 fspath write) with
      | exception Sys_error e -> S.Response.fail ~code:500 "Upload error: %s" e
      | () ->
        let opt = List.assoc_opt "content-type" req.headers in
        let content_type = Option.fold ~none:"application/octet-stream" ~some:Fun.id opt in
        let headers = [("content-type", content_type); ("Connection", "close")] in
        S.Response.make_string ~headers (Ok (Printf.sprintf "%s created" path))
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
    let perform () = let _ = Thread.create S.run_exn t in after_init () in
    Fun.protect ~finally:(fun () -> S.stop t) perform
end

let _ =
  run_test_tt_main @@ ("Run Zarr sync API tests" >::: [
  "test sync-based stores" >::
    (fun _ ->
      let rand_num = string_of_int @@ Random.int 1_000_000 in
      let tmp_dir = Filename.(concat (get_temp_dir_name ()) (rand_num ^ ".zarr")) in
      let s = FilesystemStore.create tmp_dir in

      assert_raises
        (Sys_error (Format.sprintf "%s: File exists" tmp_dir))
        (fun () -> FilesystemStore.create tmp_dir);

      (* inject a bad metadata document to test correct parsing of bad child
         nodes when discovering children of a group. *)
      let dname = tmp_dir ^ "/badnode" in
      let fname = Filename.concat dname "zarr.json" in
      Sys.mkdir dname 0o700;
      Out_channel.with_open_bin
        fname
        (Fun.flip Out_channel.output_string {|{"zarr_format":3,"node_type":"unknown"}|});
      assert_raises
        (Zarr.Metadata.Parse_error "invalid node_type in badnode/zarr.json")
        (fun () -> FilesystemStore.hierarchy s);
      Sys.(remove fname; rmdir dname);

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

      (* test with non-existant archive *)
      let zpath = tmp_dir ^ ".zip" in
      ZipStore.with_open `Read_write zpath (fun z -> test_storage (module ZipStore) z);
      (* test just opening the now exisitant archive created by the previous test. *)
      ZipStore.with_open `Read_only zpath (fun _ -> ());
      test_storage (module MemoryStore) @@ MemoryStore.create ();
      test_storage (module FilesystemStore) s;

      let server = Dir_http_server.make ~max_connections:1 ~dir:tmp_dir () in
      Dir_http_server.run_with server (fun () ->
        HttpStore.with_open "127.0.0.1:8080" (test_readable_writable_only (module HttpStore)))
    )
  ])
