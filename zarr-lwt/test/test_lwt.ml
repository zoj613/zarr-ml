open OUnit2
open Zarr
open Zarr.Indexing
open Zarr.Codecs
open Zarr_lwt.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: Node.Array.t list * Node.Group.t list]
let fold_result r = Result.fold ~ok:Fun.id ~error:(fun _ -> assert_failure "This should not fail.") r

module Make (E: sig type t end) (M : Storage.S with type 'a io := 'a Lwt.t and type error := E.t) = struct
  open M
  open IO.Syntax

  let test_all store =
    let gnode = Node.Group.root in
    let* nodes = hierarchy store in
    assert_equal ~printer:print_node_pair ([], []) nodes;

    let* () = Group.create store gnode in
    let* exists = Group.exists store gnode in
    assert_equal ~printer:string_of_bool true exists;
    let* nodes = hierarchy store in
    assert_equal ~printer:print_node_pair ([], [gnode]) nodes;

    let* meta = Group.metadata store gnode in
    assert_equal ~printer:Metadata.Group.show Metadata.Group.default meta;

    let* () = Group.delete store gnode in
    let* exists = Group.exists store gnode in
    assert_equal ~printer:string_of_bool false exists;
    let* nodes = hierarchy store in
    assert_equal ~printer:print_node_pair ([], []) nodes;

    let attrs = `Assoc [("questions", `String "answer")] in
    let* () = Group.create ~attrs store gnode in
    let* meta = Group.metadata store gnode in
    assert_equal ~printer:Yojson.Safe.show attrs @@ Metadata.Group.attributes meta;

    let* exists = IO.bind (IO.lift Node.Array.(gnode / "non-member")) (Array.exists store) in
    assert_equal ~printer:string_of_bool false exists;

    let cfg =
      {chunk_shape = [2; 5; 5]
      ;index_location = End
      ;index_codecs = [`Bytes BE]
      ;codecs = [`Bytes LE]} in
    let slice = [R (0, 20); I 10; R (0, 29)] in
    let bigger_slice =  [R (0, 21); L [9; 10] ; R (0, 30)] in
    let anode = fold_result Node.Array.(gnode / "arrnode") in

    let* () = Array.create ~codecs:[`ShardingIndexed cfg] ~shape:[100; 100; 50] ~chunks:[10; 15; 20] Complex32 Complex.one anode store in
    let exp = Ndarray.init Complex32 [21; 1; 30] (Fun.const Complex.one) in
    let* got = Array.read store anode slice Complex32 in
    assert_equal exp got;
    Ndarray.fill exp Complex.{re=2.0; im=0.};
    let* () = Array.write store anode slice exp in
    let* got = Array.read store anode slice Complex32 in
    (* test if a bigger slice containing new elements can be read from store *)
    let* _ = Array.read store anode bigger_slice Complex32 in
    assert_equal exp got;
    (* test writing a bigger slice to store *)
    let* () = Array.write store anode bigger_slice (Ndarray.init Complex32 [22; 2; 31] (Fun.const Complex.{re=0.; im=3.0})) in
    let* got = Array.read store anode slice Complex32 in
    Ndarray.fill exp Complex.{re=0.; im=3.0};
    assert_equal exp got;

    let child = fold_result (Node.Group.of_path "/some/child/group") in
    let* () = Group.create store child in
    let* arrays, groups = Group.children store gnode in
    assert_equal ~printer:string_of_list ["/arrnode"] (List.map Node.Array.to_path arrays);
    assert_equal ~printer:string_of_list ["/some"] (List.map Node.Group.to_path groups);
    let* got = Group.children store child in
    assert_equal ([], []) got;
    let* got = Group.children store (fold_result Node.Group.(root / "fakegroup")) in
    assert_equal ([], []) got;

    let* ac, gc = hierarchy store in
    let got = List.fast_sort String.compare (List.map Node.Array.show ac @ List.map Node.Group.show gc) in
    assert_equal ~printer:string_of_list ["/"; "/arrnode"; "/some"; "/some/child"; "/some/child/group"] got;

    (* tests for renaming nodes *)
    let some = fold_result (Node.Group.of_path "/some/child") in
    let* _ = Group.rename store some "CHILD" in
    let* anode' = Array.rename store anode "ARRAYNODE" in
    let* ac, gc = hierarchy store in
    let got = List.fast_sort String.compare (List.map Node.Array.show ac @ List.map Node.Group.show gc) in
    assert_equal ~printer:string_of_list ["/"; "/ARRAYNODE"; "/some"; "/some/CHILD"; "/some/CHILD/group"] got;
    (* restore old array node name. *)
    let* got = Array.rename store anode' "arrnode" in
    assert_equal anode got;
    let nshape = [25; 32; 10] in
    let* () = Array.resize store anode nshape in
    let* meta = Array.metadata store anode in
    assert_equal ~printer:[%show : int list] nshape (Metadata.Array.shape meta);

    let* () = Array.delete store anode in
    let* () = clear store in
    let* got = hierarchy store in
    assert_equal ~printer:print_node_pair ([], []) got;
    IO.return_unit
end

let test_zip_store () =
  let module ZipStoreTester = Make (struct type t = Zip.error end) (ZipStore) in
  Random.self_init ();
  let tmp_dir = Filename.(concat (get_temp_dir_name ()) ((string_of_int @@ Random.int 100) ^ ".zarr")) in
  (* test with non-existant archive *)
  let zpath = tmp_dir ^ ".zip" in
  let res1 = Lwt.bind (Lwt_result.bind (ZipStore.create zpath) ZipStoreTester.test_all) @@ function
    | Ok () -> Lwt.return_unit
    | _ -> assert_failure "zipstore test suite is not supposed to fail"
  in
  let res2 = Lwt_list.iter_p
    (fun level -> let _ = fold_result (ZipStore.open_store ~level zpath) in Lwt.return_unit)
    [L0; L1; L2; L3; L4; L5; L7; L8; L9]
  in
  Lwt.join [res1; res2]

let test_memory_store () =
  let module MemStoreTester = Make (struct type t = Memory.error end) (MemoryStore) in
  Lwt.bind (MemStoreTester.test_all (MemoryStore.create ())) @@ function
  | Ok () -> Lwt.return_unit
  | Error _ -> assert_failure "memorystore test suite is not supposed to fail"

let test_filesystem_store () =
  let module FSStoreTester = Make (struct type t = FilesystemStore.error end) (FilesystemStore) in
  Random.self_init ();
  let rand_num = string_of_int @@ Random.int 100 in
  let tmp_dir = Filename.(concat (get_temp_dir_name ()) (rand_num ^ ".zarr")) in
  Lwt.bind (FilesystemStore.create tmp_dir) @@ function
  | Error _ -> assert_failure "failed to create filesystem store"
  | Ok s -> Lwt.bind (FilesystemStore.create tmp_dir) @@ function
    | Ok _ -> assert_failure "cannot create a Lwt-based FilesystemStore using an existing path"
    | Error (`Zarr (`Write _)) -> 
      let _ = fold_result (FilesystemStore.open_store (tmp_dir ^ "/")) in
      let fakedir = "non-existant.zarr" in
      assert_equal
        (Error (`Zarr (`Read (Printf.sprintf "%s: No such file or directory" fakedir))))
        (FilesystemStore.open_store fakedir);
      Lwt.bind (FSStoreTester.test_all s) @@ function
      | Error _ -> assert_failure "filesystem test suite is not supposed to fail"
      | Ok () ->
        let fn = tmp_dir ^ "/zarr.json" in
        Lwt.bind
          (Lwt_io.with_file
            ~flags:Unix.[O_WRONLY; O_TRUNC; O_CREAT] ~perm:0o700 ~mode:Lwt_io.Output fn
            (fun oc -> Lwt_io.write oc fn))
          (fun () ->
            assert_equal
              (Error (`Zarr (`Read (Printf.sprintf "%s is not a directory." fn))))
              (FilesystemStore.open_store fn);
              Lwt_unix.unlink fn)

let test_s3_store () =
  let module S3StoreTester = Make (struct type t = AmazonS3Store.error end) (AmazonS3Store) in
  let region = Aws_s3.Region.minio ~port:9000 ~host:"localhost" ()
  and bucket = "test-bucket-lwt"
  and profile = "default" in
  Lwt.bind (AmazonS3Store.with_open ~region ~bucket ~profile S3StoreTester.test_all) @@ function
  | Ok () -> Lwt.return_unit
  | Error _ -> assert_failure "s3store test suite is not supposed to fail"

let _ = 
  run_test_tt_main @@ ("Run Zarr Lwt API test suite" >::: [
    "Run Zarr Lwt API test suite" >:: (fun _ ->
      let xs = 
        [test_filesystem_store ()
        ;test_memory_store ()
        ;test_s3_store ()
        ;test_zip_store ()] in
      Lwt_main.run @@ Lwt.join xs
    )
])
