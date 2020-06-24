import numpy as np
import pandas as pd

# For multi-threaded groupby
from multiprocessing import Pool, cpu_count


def applyParallel(dfGrouped, func):
    with Pool(cpu_count()) as p:
        ret_list = p.map(func, [group for name, group in dfGrouped])
    return pd.concat(ret_list)


class Groupby:
    def __init__(self, keys):
        """

        :param keys: List of group identifiers. Both __init__ and apply will run
            much faster if keys is already sorted.
        """
        try:
            already_sorted = np.issubdtype(keys.dtype, np.number) and (np.all(np.diff(keys) >= 0))
        except ValueError:
            already_sorted = False
        if already_sorted:
            print("Groupby is already sorted...good!\n")
            keys = np.squeeze(keys)
            if keys.ndim > 1:
                raise ValueError('keys should be 1-dimensional')

            self.already_sorted = True
            new_idx = np.concatenate(([1], np.diff(keys) != 0))
            self.first_occurrences = np.where(new_idx)[0]
            self.keys_as_int = np.cumsum(new_idx) - 1
            assert isinstance(self.keys_as_int, np.ndarray)
            self.n_keys = self.keys_as_int[-1] + 1

        else:
            self.already_sorted = False
            _, self.first_occurrences, self.keys_as_int = \
                np.unique(keys, return_index=True, return_inverse=True)
            self.n_keys = max(self.keys_as_int) + 1
        self.indices = self._set_indices()

    def _set_indices(self):
        if self.already_sorted:
            indices = [slice(i, j) for i, j in zip(self.first_occurrences[:-1],
                                                   self.first_occurrences[1:])]
            assert isinstance(indices, list)
            indices.append(slice(self.first_occurrences[-1], len(self.keys_as_int)))
            indices = np.array(indices)
        else:
            indices = [[] for _ in range(self.n_keys)]
            for i, k in enumerate(self.keys_as_int):
                indices[k].append(i)
            indices = np.array([np.array(elt) for elt in indices])
        return indices

    def apply(self, function_, array, broadcast=True, shape=None, order='c'):
        """
        Applies a function to each group, where groups are defined by self.keys_as_int (or, equivalently, as the
            argument of __init__.)
        If broadcast=True, first dimension of output will equal first dimension of "array", as in Pandas "transform".
        If broadcast=False, first dimension of output equals self.n_keys, as in Pandas "groupby".

        :param function_: function to be applied to each group
        :param array: np.ndarray or similar. Should have same first dimension as self.keys_as_int.
        :param broadcast: bool
        :param shape: Shape of output. Can be up to 3-dimensional.
            First dimension must be array.shape[0] (if broadcast=True)
            or self.n_keys (if broadcast=False). Default is for output to be one-dimensional.
        :param order: Should output be c-ordered or fortran-ordered?
        :return:
        :rtype: np.ndarray
        """
        if broadcast:
            result = np.zeros(array.shape[0] if shape is None else shape, order=order)
            assert result.shape[0] == array.shape[0]

            # np.take doesn't allow slice arguments, so this has to be more verbose than when not already sorted
            if self.already_sorted:
                if array.ndim == 1:
                    for k, idx in enumerate(self.indices):
                        result[idx] = function_(array[idx])
                elif array.ndim == 2:
                    for k, idx in enumerate(self.indices):
                        result[idx] = function_(array[idx, :])
                elif array.ndim == 3:
                    for k, idx in enumerate(self.indices):
                        result[idx] = function_(array[idx, :, :])
                else:
                    raise NotImplementedError('Can\'t have more than 3 dims')
            else:
                for k, idx in enumerate(self.indices):
                    result[idx] = function_(np.take(array, idx, 0))

        else:
            result = np.zeros(self.n_keys if shape is None else shape, order=order)
            assert result.shape[0] == self.n_keys
            if self.already_sorted:
                if array.ndim == 1:
                    for k, idx in enumerate(self.indices):
                        result[k] = function_(array[idx])
                elif array.ndim == 2:
                    for k, idx in enumerate(self.indices):
                        result[k] = function_(array[idx, :])
                elif array.ndim == 3:
                    for k, idx in enumerate(self.indices):
                        result[k] = function_(array[idx, :, :])
                else:
                    raise NotImplementedError('Can\'t have more than 3 dims')

            else:
                for k, idx in enumerate(self.indices):
                    result[self.keys_as_int[self.first_occurrences[k]]] \
                        = function_(np.take(array, idx, 0))

        return result