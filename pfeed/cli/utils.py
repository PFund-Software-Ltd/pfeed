def parse_extra_args(args):
    """
    args is ctx.args where ctx is a click.Context object.
    Parse extra arguments of the form --key value into a dictionary.
    If a flag is provided without a value (e.g. --flag), it's set to True.
    """
    extra_kwargs = {}
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.startswith("--"):
            # Replace dashes with underscores
            key = arg[2:].replace("-", "_")
            # Check if there's a following value that isn't another option.
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                extra_kwargs[key] = args[i + 1]
                i += 2
            else:
                extra_kwargs[key] = True
                i += 1
        else:
            i += 1
    return extra_kwargs