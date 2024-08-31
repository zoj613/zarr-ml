module ExtPoint = struct
  type 'a t =
    {name : string
    ;configuration : 'a}

  let ( = ) cmp x y =
    (x.name = y.name) &&
    cmp x.configuration y.configuration
end

module StrSet = Set.Make(String)

module ArrayMap = struct
  include Map.Make (struct
    type t = int array
    let compare (x : t) (y : t) = Stdlib.compare x y
  end)

  let add_to_list k v map =
    update k (Option.fold ~none:(Some [v]) ~some:(fun l -> Some (v :: l))) map
end

module Result_syntax = struct
  let (let*) = Result.bind
  let (let+) x f = Result.map f x
end

module Indexing = struct
  let rec cartesian_prod = function
    | [] -> [[]]
    | x :: xs ->
      List.concat_map (fun i ->
        List.map (List.cons i) (cartesian_prod xs)) x

  let range ?(step=1) start stop =
    List.of_seq @@ if step > 0 then
      Seq.unfold (function
        | x when x > stop -> None
        | x -> Some (x, x + step)) start
    else
      let start, stop = stop, start in
      Seq.unfold (function
        | x when x < start -> None
        | x -> Some (x, x + step)) stop

  (* get indices from a reformated slice *)
  let indices_of_slice = function
    | Owl_types.R_ [|start; stop; step|] -> range ~step start stop
    | Owl_types.L_ l -> Array.to_list l
    (* this is added for exhaustiveness but is never reached since
      a reformatted slice replaces a I_ index with an R_ index.*)
    | _ -> failwith "Invalid slice index."

  let reformat_slice slice shape =
    match slice with
    | [||] -> [||]
    | xs ->
      Owl_slicing.check_slice_definition
        (Owl_slicing.sdarray_to_sdarray xs) shape

  let coords_of_slice slice shape =
    (Array.map indices_of_slice @@
      reformat_slice slice shape)
    |> Array.to_list
    |> cartesian_prod
    |> List.map Array.of_list
    |> Array.of_list

   module IntSet = Set.Make(Int)

  let slice_of_coords = function
    | [] -> [||]
    | x :: _ as xs ->
      let ndims = Array.length x in
      let indices = Array.make ndims IntSet.empty in
      Array.map (fun x -> Owl_types.L (IntSet.elements x)) @@
      List.fold_right (fun x acc ->
        Array.iteri (fun i y ->
          if IntSet.mem y acc.(i) then ()
          else acc.(i) <- IntSet.add y acc.(i)) x; acc) xs indices

  let slice_shape slice array_shape =
    Owl_slicing.calc_slice_shape @@
      reformat_slice slice array_shape
end

let get_name j =
  Yojson.Safe.Util.(member "name" j |> to_string)

let prod x =
  Array.fold_left Int.mul 1 x

let max x =
  Array.fold_left
    (fun acc v ->
      if v <= acc then acc else v) Int.min_int x

(* Obtained from: https://discuss.ocaml.org/t/how-to-create-a-new-file-while-automatically-creating-any-intermediate-directories/14837/5?u=zoj613 *)
let rec create_parent_dir fn perm =
  let parent_dir = Filename.dirname fn in
  if not (Sys.file_exists parent_dir) then begin
    create_parent_dir parent_dir perm;
    Sys.mkdir parent_dir perm
  end

let sanitize_dir dir =
  Option.fold ~none:dir ~some:Fun.id @@ Filename.chop_suffix_opt ~suffix:"/" dir
