from viztracer import VizTracer, get_tracer


def profiler(filename: str) -> VizTracer:
    return VizTracer(output_file=filename)


def log_event(name: str):
    return get_tracer().log_event(name)
