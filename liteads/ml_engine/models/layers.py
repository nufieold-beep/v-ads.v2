"""
Reusable neural network layers for CTR prediction models.

Extracted from DeepFM for independent testing and reuse across model variants.
"""

from __future__ import annotations

import torch
import torch.nn as nn


class EmbeddingLayer(nn.Module):
    """Multi-feature embedding layer that concatenates per-feature embeddings.

    Args:
        feature_dims: Vocabulary size for each sparse feature.
        embedding_dims: Embedding dimension for each feature (same length).
    """

    def __init__(self, feature_dims: list[int], embedding_dims: list[int]):
        super().__init__()
        self.embeddings = nn.ModuleList([
            nn.Embedding(vocab, dim)
            for vocab, dim in zip(feature_dims, embedding_dims)
        ])
        for emb in self.embeddings:
            nn.init.xavier_uniform_(emb.weight)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Args:
            x: (batch_size, num_features) — integer indices per feature.

        Returns:
            (batch_size, sum(embedding_dims)) — concatenated embeddings.
        """
        parts = [emb(x[:, i]) for i, emb in enumerate(self.embeddings)]
        return torch.cat(parts, dim=1)


class SequenceEmbeddingLayer(nn.Module):
    """Sequence embedding using EmbeddingBag (mean/sum pooling).

    Args:
        vocab_size: Vocabulary size.
        embedding_dim: Embedding dimension.
        mode: Pooling mode — ``"mean"``, ``"sum"``, or ``"max"``.
    """

    def __init__(self, vocab_size: int, embedding_dim: int, mode: str = "mean"):
        super().__init__()
        self.bag = nn.EmbeddingBag(vocab_size, embedding_dim, mode=mode)

    def forward(
        self, values: torch.Tensor, offsets: torch.Tensor,
    ) -> torch.Tensor:
        """
        Args:
            values:  1-D tensor of token indices.
            offsets: 1-D tensor marking the start of each sequence.

        Returns:
            (num_sequences, embedding_dim)
        """
        return self.bag(values, offsets)


class FM(nn.Module):
    """Factorization Machine interaction layer.

    Computes pairwise 2nd-order interactions efficiently:
    ``0.5 * (sum(vi·xi)^2 − sum(vi^2·xi^2))``

    Accepts a *list* of per-feature embedding tensors (each ``(batch, dim)``).
    """

    def __init__(self, reduce_sum: bool = True):
        super().__init__()
        self.reduce_sum = reduce_sum

    def forward(self, embeddings: list[torch.Tensor]) -> torch.Tensor:
        """
        Args:
            embeddings: List of (batch_size, embedding_dim) tensors.

        Returns:
            (batch_size, 1) if reduce_sum else (batch_size, embedding_dim).
        """
        stacked = torch.stack(embeddings, dim=1)  # (B, N, D)
        sum_square = torch.sum(stacked, dim=1) ** 2
        square_sum = torch.sum(stacked ** 2, dim=1)
        output = 0.5 * (sum_square - square_sum)
        if self.reduce_sum:
            output = torch.sum(output, dim=1, keepdim=True)
        return output


class DNN(nn.Module):
    """Deep Neural Network with optional output projection.

    Args:
        input_dim: Input feature dimension.
        hidden_units: List of hidden layer sizes.
        dropout: Dropout rate.
        activation: One of ``"relu"``, ``"leaky_relu"``, ``"gelu"``, ``"tanh"``.
        output_dim: If provided, appends a final Linear(last_hidden, output_dim).
    """

    _ACTIVATIONS = {
        "relu": nn.ReLU,
        "leaky_relu": nn.LeakyReLU,
        "gelu": nn.GELU,
        "tanh": nn.Tanh,
    }

    def __init__(
        self,
        input_dim: int,
        hidden_units: list[int],
        dropout: float = 0.0,
        activation: str = "relu",
        output_dim: int | None = None,
    ):
        super().__init__()
        act_cls = self._ACTIVATIONS.get(activation, nn.ReLU)

        layers: list[nn.Module] = []
        prev = input_dim
        for h in hidden_units:
            layers.append(nn.Linear(prev, h))
            layers.append(nn.BatchNorm1d(h))
            layers.append(act_cls())
            if dropout > 0:
                layers.append(nn.Dropout(dropout))
            prev = h

        if output_dim is not None:
            layers.append(nn.Linear(prev, output_dim))
            prev = output_dim

        self.dnn = nn.Sequential(*layers)
        self.output_dim = prev

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.dnn(x)
