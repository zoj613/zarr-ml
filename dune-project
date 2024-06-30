(lang dune 3.15)

(name zarr)

(generate_opam_files true)

(source
 (github zoj613/zarr-ml))

(authors "Zolisa Bleki")

(maintainers "Zolisa Bleki")

(license BSD-3-Clause)

(documentation https://zoj613.github.io/zarr/zarr/Zarr/index.html)

(package
 (name zarr)
 (synopsis "A short synopsis")
 (description "A longer description")
 (depends
   dune
   (ocaml (>= 4.14.2))
   yojson
   ppx_deriving_yojson
   ezgzip
   checkseum
   stdint
   (ounit2 :with-test)
   (odoc :with-doc)
   (bisect_ppx
     (and :dev (>= 2.5.0) :with-test))
   owl)
 (tags
  (topics "to describe" your project)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
