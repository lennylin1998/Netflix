from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


def multi_thread(func, input, n: int):
    """
    :param func: The function to be submitted to the multi-thread workers.
    :param input: The list of input argument(or dataframe) for the function being submitted.
    :param n: How many input arguments are in a batch.
    :return: A combined dataframe after the multi-thread work.

    **Note: The function being submitted should have a return type of dataframe.
    """
    batch_list = []
    if type(input) == list:
        batch_list = [input[i * n: (i + 1) * n] for i in range(len(input) // n)]
        if len(input) % n:
            batch_list.append(input[-(len(input) % n):])
    elif type(input) == pd.DataFrame:
        batch_list = [input.iloc[i * n: (i+1) * n] for i in range(input.shape[0] // n)]
        if input.shape[0] % n:
            batch_list.append(input.iloc[-(input.shape[0] % n):])
    else:
        raise TypeError("The input argument should be type 'list' or 'DataFrame'.")
    print("Batch List: ", batch_list)
    futures = []
    with ThreadPoolExecutor(max_workers=None) as executor:
        for batch in batch_list:
            future = executor.submit(func, batch)
            futures.append(future)
    completed = list(as_completed(futures))
    df = completed[0].result()
    for future in completed[1:]:
        df = df.append(future.result(), ignore_index=True)
    return df

