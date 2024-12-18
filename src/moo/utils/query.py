SWAP_QUERY = """
    {{
        swaps (first: 1000, skip: {skip}, orderBy: blockTimestamp, order: desc){{
        blockTimestamp
        transactionHash
        tokenAmountIn
        tokenAmountOut
        tokenIn
        tokenInSymbol
        tokenOutSymbol
        tokenOut
    }}
}}
"""
