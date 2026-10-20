fn main() {
    lalrpop::process_src().expect("failed to generate parser from LALRPOP grammar");
}
