import glob,sys,os,pdb,concurrent.futures,time,warnings

def run_function_in_parallel(fun,args_list):
    """
    Description:        Generic function that runs any function over a set of CPUs -
                        note that this uses concurrent.futures.PoolProcessExecutor due
                        to multithreading issues with rpy2
    Inputs:
        fun             Python function
        args_list       List of dictionaries with keys set to match the input variable
                        argument names for the function fun. Note: each dictionary must have
                        a key called ID with unique value to be identifiable in the results dictionary.
    Returns:
        results_dict    Concatenated results of the function from collection of args_list
                        indexed by the ID key in the args_list list of dictionaries. Each entry
                        has a "result" entry that is the return value of the function, a "warnings" 
                        entry, and if the function does not conclude successfully, an error entry 
                        consisting of the Exception.
    """
    result_dict = {}
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit tasks to be executed in parallel with named arguments
        future_to_args = {executor.submit(fun, **args): args for args in args_list}

        # Gather results as they complete
        for future in concurrent.futures.as_completed(future_to_args):
            args_future = future_to_args[future]
            try:
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    result = future.result()
                    result_dict[args_future['ID']] = {
                            "result": result,
                            "warnings": [str(warn.message) for warn in w],
                            "error": None
                            }
                print(f"Function with arg ID {args_future['ID']} finished successfully!")
            except Exception as exc:
                print(f"Function with arg ID {args_future['ID']} generated an exception: {exc}")
                result_dict[args_future['ID']] = {
                            "result": None,
                            "warnings": [],
                            "error": f"Generated an exception: {exc}"
                            }
    return result_dict