open OUnit2
open Zarr.Node


let group_node = [
"group node tests" >:: (fun _ ->
  let creation_failure = "Creation of group node should not fail." in
  let r = GroupNode.(root / "somename") in
  assert_bool creation_failure @@ Result.is_ok r;

  (* test node invariants *)
  let n = Result.get_ok r in
  let msg = "Creation of group node should not succeed." in
  List.iter
    (fun x -> assert_bool msg @@ Result.is_error x) @@
    List.map (GroupNode.create n) [""; "na/me"; "...."; "__name"];

  (* creation from string path *)
  let r = GroupNode.of_path "/" in
  assert_bool creation_failure @@ Result.is_ok r;
  assert_equal
    ~printer:GroupNode.show GroupNode.root @@ Result.get_ok r;
  List.iter
    (fun x -> assert_bool msg @@ Result.is_error x) @@
    List.map
      GroupNode.of_path @@
      [""; "na/meas"; "/some/..."; "/root/__name"; "/sd/"];

  (* node name tests *)
  let s = "/some/dir/moredirs/path/pname" in
  let n = GroupNode.of_path s |> Result.get_ok in
  assert_equal "pname" @@ GroupNode.name n;
  assert_equal "" @@ GroupNode.name GroupNode.root;

  (* parent tests *)
  assert_equal None @@ GroupNode.parent GroupNode.root;
  match GroupNode.parent n with
  | None ->
    assert_failure "A non-root node must have a parent.";
  | Some p ->
    assert_equal "/some/dir/moredirs/path" @@ GroupNode.show p;

  (* equality tests *)
  assert_equal ~printer:GroupNode.show GroupNode.root GroupNode.root;
  assert_bool
    "root node cannot be equal to its child" @@
    not GroupNode.(root = n);
  assert_bool
    "non-root node cannot have root as child" @@
    not GroupNode.(n = root);

  (* ancestory tests *)
  assert_equal [] @@ GroupNode.ancestors GroupNode.root;
  assert_equal
    ~printer:[%show: string list]
    ["/"; "/some"; "/some/dir"; "/some/dir/moredirs"
    ;"/some/dir/moredirs/path"]
    (GroupNode.ancestors n |> List.map GroupNode.show);
  let exp_parents = GroupNode.ancestors n in
  let r, l = List.fold_left_map
    (fun acc _ ->
      match GroupNode.parent acc with
      | Some acc' -> acc', acc'
      | None -> acc, acc) n exp_parents
  in
  assert_equal
    ~printer:[%show: GroupNode.t list]
    exp_parents @@
    List.rev l;
  assert_equal ~printer:GroupNode.show r GroupNode.root;

  (* child node tests *)
  let p = GroupNode.parent n |> Option.get in
  assert_equal
    ~printer:string_of_bool
    true @@
    GroupNode.is_child_group p n;
  assert_equal
    ~printer:string_of_bool
    false @@
    GroupNode.is_child_group n GroupNode.root;
  assert_equal
    ~printer:string_of_bool
    false @@
    GroupNode.is_child_group GroupNode.root GroupNode.root;

  (* stringify tests *)
  assert_equal
    ~printer:Fun.id "" @@ GroupNode.to_key GroupNode.root;

  assert_equal
    ~printer:Fun.id
    "some/dir/moredirs/path/pname" @@
    GroupNode.to_key n;

  assert_equal
    ~printer:Fun.id
    "zarr.json" @@
    GroupNode.to_metakey GroupNode.root;

  assert_equal
    ~printer:Fun.id
    ("some/dir/moredirs/path/pname/zarr.json") @@
    GroupNode.to_metakey n)
]

let array_node = [
"array node tests" >:: (fun _ ->
  let r = ArrayNode.(GroupNode.root / "somename") in
  assert_bool "" @@ Result.is_ok r;

  (* test node invariants *)
  let msg = "Creation of group node should not succeed." in
  List.iter
    (fun x -> assert_bool msg @@ Result.is_error x) @@
    List.map
      (ArrayNode.create GroupNode.root)
      [""; "na/me"; "...."; "__name"];

  (* creation from string path *)
  let msg = "creating an array node from an illformed path is impossible" in
  List.iter
    (fun x -> assert_bool msg @@ Result.is_error x) @@
    List.map
      ArrayNode.of_path @@
      ["/"; ""; "na/meas"; "/some/..."; "/root/__name"; "/sd/"];

  (* node name tests *)
  let s = "/some/dir/moredirs/path/pname" in
  let n = ArrayNode.of_path s |> Result.get_ok in
  assert_equal "pname" @@ ArrayNode.name n;
  assert_equal ~printer:Fun.id s @@ ArrayNode.to_path n;

  (* parent tests *)
  assert_equal
    ~printer:GroupNode.show
    GroupNode.root @@
    ArrayNode.parent (ArrayNode.of_path "/nodename" |> Result.get_ok);

  (* equality tests *)
  assert_equal
    true @@ ArrayNode.(n = (ArrayNode.of_path s |> Result.get_ok));
  assert_equal
    false @@
    ArrayNode.(n = Result.get_ok @@ ArrayNode.of_path (s ^ "/more"));

  (* ancestory tests *)
  assert_equal
    ~printer:[%show: string list]
    ["/"; "/some"; "/some/dir"; "/some/dir/moredirs"
    ;"/some/dir/moredirs/path"]
    (ArrayNode.ancestors n
      |> List.map GroupNode.show
      |> List.fast_sort String.compare);
  let m = ArrayNode.of_path "/some" |> Result.get_ok in
  assert_equal true @@ ArrayNode.is_parent m GroupNode.root;

  (* stringify tests *)
  assert_equal
    ~printer:Fun.id
    "some/dir/moredirs/path/pname" @@
    ArrayNode.to_key n;

  assert_equal
    ~printer:Fun.id
    ("some/dir/moredirs/path/pname/zarr.json") @@
    ArrayNode.to_metakey n)
]

let tests = group_node @ array_node
