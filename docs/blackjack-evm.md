## Blackjack EVM Setup

This project now supports a non-custodial blackjack flow.

### Contract

Deploy `contracts/BlackjackEscrowFactory.sol` on each supported chain you want to use.

The server expects the contract ABI to expose:

- `createMatch(bytes32,address,uint256,address[],uint64)`
- `deposit(bytes32)`
- `finalizeMatch(bytes32,address)`
- `claim(bytes32,address)`
- `getMatch(bytes32)`

### Server env

Set these in Railway or your server environment:

```env
CHAIN_MODE=evm
BLACKJACK_OPERATOR_PRIVATE_KEY=0x...

BASE_RPC_URL=https://...
ARBITRUM_RPC_URL=https://...
POLYGON_RPC_URL=https://...

BLACKJACK_FACTORY_ADDRESS_BASE=0x...
BLACKJACK_FACTORY_ADDRESS_ARBITRUM=0x...
BLACKJACK_FACTORY_ADDRESS_POLYGON=0x...

BLACKJACK_TOKEN_USDC_BASE=0x...
BLACKJACK_TOKEN_USDT_BASE=0x...
BLACKJACK_TOKEN_DAI_BASE=0x...

BLACKJACK_TOKEN_USDC_ARBITRUM=0x...
BLACKJACK_TOKEN_USDT_ARBITRUM=0x...
BLACKJACK_TOKEN_DAI_ARBITRUM=0x...

BLACKJACK_TOKEN_USDC_POLYGON=0x...
BLACKJACK_TOKEN_USDT_POLYGON=0x...
BLACKJACK_TOKEN_DAI_POLYGON=0x...
```

### Client env

Set this in Netlify or the client environment:

```env
VITE_CHAIN_MODE=evm
VITE_API_URL=https://your-api-host
```

### Flow

1. Users accept a blackjack invite.
2. Each user connects a wallet and registers the address.
3. The server creates the escrow match on-chain once all wallets are registered.
4. Users approve the token and call `deposit`.
5. The server finalizes the winner when the blackjack round ends.
6. The winner calls `claim(matchId, recipient)` from their wallet.

### Notes

- This implementation is ERC20-only for now.
- The operator key only creates/finalizes matches. User deposits and claims stay in the users' wallets.
- If you want stronger trust minimization later, replace owner-finalize with an on-chain verifier or server-signed authorization checked in the contract.
