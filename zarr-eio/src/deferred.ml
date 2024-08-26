type 'a t = 'a
let return = Fun.id
let return_unit = ()
let iter f xs = Eio.Fiber.List.iter f xs
let fold_left = List.fold_left
let concat_map f xs = List.concat @@ Eio.Fiber.List.map f xs

module Infix = struct
  let (>>=) x f = f x
  let (>>|) = (>>=)
end
