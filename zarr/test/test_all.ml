open OUnit2

let () =
  let suite = "Run All tests" >:::
      Test_node.tests @
      Test_indexing.tests @
      Test_metadata.tests @
      Test_codecs.tests
  in run_test_tt_main suite
