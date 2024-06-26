open OUnit2
open Zarr

let creation_failure = "Creation of node should not fail." 

let tests = [

"creation" >:: (fun _ ->
  let n = Node.(root / "somename") in
  assert_bool creation_failure @@ Result.is_ok n;
  let p = Result.get_ok n in
  let msg = "Creation of node should not succeed." in
  List.iter
    (fun x -> assert_bool msg @@ Result.is_error x) @@
    List.map (Node.create p) [""; "na/me"; "...."; "__name"])
;
"creation from path string" >:: (fun _ ->
  let n = Node.of_path "/" in
  assert_bool creation_failure @@ Result.is_ok n;
  assert_equal "/" @@ Node.to_path @@ Result.get_ok n;
  let msg = "Creation of node should not succeed" in
  List.iter
    (fun x -> assert_bool msg @@ Result.is_error x) @@
    List.map Node.of_path [""; "na/meas"; "/some/..."; "/root/__name"; "/sd/"])
;
"exploratory functions" >:: (fun _ ->
  let s = "/some/dir/moredirs/path/pname" in
  let n = Node.of_path s |> Result.get_ok in
  assert_equal "pname" @@ Node.name n;
  assert_equal "" @@ Node.name Node.root;

  assert_equal None @@ Node.parent Node.root;
  match Node.parent n with
  | None -> assert_bool "A non-root node must have a parent." false;
  | Some p -> assert_equal "/some/dir/moredirs/path" @@ Node.to_path p;

  assert_bool "" Node.(root = root);
  assert_bool "root node cannot be equal to its child" @@ not Node.(root = n);
  assert_bool "non-root node cannot have root as child" @@ not Node.(n = root);

  assert_equal [] @@ Node.ancestors Node.root;
  assert_equal
    ["/"; "/some"; "/some/dir"; "/some/dir/moredirs"; "/some/dir/moredirs/path"]
    (Node.ancestors n |> List.map Node.to_path);

  let p = Node.parent n |> Option.get in
  assert_bool "" @@ Node.is_parent n p;
  assert_bool "" @@ not @@ Node.is_parent Node.root n;
  assert_bool "" @@ not @@ Node.is_parent Node.root Node.root;

  let exp_parents = Node.ancestors n in
  let r, l = List.fold_left_map 
    (fun acc _ ->
      match Node.parent acc with
      | Some acc' -> acc', acc'
      | None -> acc, acc) n exp_parents in
  assert_bool "" (exp_parents = List.rev l);
  assert_equal r Node.root;

  assert_equal "" @@ Node.to_key Node.root;
  let exp_key = "some/dir/moredirs/path/pname" in
  assert_equal exp_key @@ Node.to_key n;

  assert_equal "zarr.json" @@ Node.to_metakey Node.root;
  assert_equal (exp_key ^ "/zarr.json") @@ Node.to_metakey n)
]
