// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface IERC20Mini {
    function transfer(address to, uint256 value) external returns (bool);
    function transferFrom(address from, address to, uint256 value) external returns (bool);
}

contract MiniGameEscrowFactory {
    struct MatchData {
        address token;
        uint256 fixedWager;
        uint64 depositDeadline;
        bool variableDeposit;
        address winner;
        bool finalized;
        bool settled;
        address[] players;
    }

    address public owner;
    mapping(bytes32 => MatchData) private matchesById;
    mapping(bytes32 => mapping(address => uint256)) public deposits;
    mapping(bytes32 => mapping(address => bool)) public isPlayer;

    event MatchCreated(bytes32 indexed matchId, address indexed token, uint256 fixedWager, bool variableDeposit);
    event Deposited(bytes32 indexed matchId, address indexed player, uint256 amount);
    event MatchFinalized(bytes32 indexed matchId, address indexed winner, uint256 totalPot);
    event Claimed(bytes32 indexed matchId, address indexed winner, address indexed recipient, uint256 totalPot);
    event Refunded(bytes32 indexed matchId, uint256 totalPot);

    modifier onlyOwner() {
        require(msg.sender == owner, "not-owner");
        _;
    }

    constructor() {
        owner = msg.sender;
    }

    function transferOwnership(address nextOwner) external onlyOwner {
        require(nextOwner != address(0), "bad-owner");
        owner = nextOwner;
    }

    function createMatch(
        bytes32 matchId,
        address token,
        uint256 fixedWager,
        bool variableDeposit,
        address[] calldata players,
        uint64 depositDeadline
    ) external onlyOwner {
        require(matchesById[matchId].token == address(0), "match-exists");
        require(token != address(0), "bad-token");
        require(variableDeposit || fixedWager > 0, "bad-wager");
        require(players.length >= 2, "need-players");
        require(depositDeadline > block.timestamp, "bad-deadline");

        MatchData storage m = matchesById[matchId];
        m.token = token;
        m.fixedWager = fixedWager;
        m.depositDeadline = depositDeadline;
        m.variableDeposit = variableDeposit;

        for (uint256 i = 0; i < players.length; i++) {
            require(players[i] != address(0), "bad-player");
            require(!isPlayer[matchId][players[i]], "dup-player");
            isPlayer[matchId][players[i]] = true;
            m.players.push(players[i]);
        }

        emit MatchCreated(matchId, token, fixedWager, variableDeposit);
    }

    function deposit(bytes32 matchId, uint256 amount) external {
        MatchData storage m = matchesById[matchId];
        require(m.token != address(0), "match-missing");
        require(!m.finalized, "finalized");
        require(block.timestamp <= m.depositDeadline, "deposit-closed");
        require(isPlayer[matchId][msg.sender], "not-player");
        require(deposits[matchId][msg.sender] == 0, "already-deposited");

        uint256 depositAmount = m.variableDeposit ? amount : m.fixedWager;
        require(depositAmount > 0, "bad-amount");

        deposits[matchId][msg.sender] = depositAmount;
        bool ok = IERC20Mini(m.token).transferFrom(msg.sender, address(this), depositAmount);
        require(ok, "transfer-failed");

        emit Deposited(matchId, msg.sender, depositAmount);
    }

    function finalizeMatch(bytes32 matchId, address winner) external onlyOwner {
        MatchData storage m = matchesById[matchId];
        require(m.token != address(0), "match-missing");
        require(!m.finalized, "already-finalized");
        require(isPlayer[matchId][winner], "winner-not-player");

        uint256 totalPot = 0;
        for (uint256 i = 0; i < m.players.length; i++) {
            uint256 amount = deposits[matchId][m.players[i]];
            if (m.variableDeposit) {
                require(amount > 0, "missing-deposit");
            } else {
                require(amount == m.fixedWager, "missing-deposit");
            }
            totalPot += amount;
        }

        m.finalized = true;
        m.winner = winner;
        emit MatchFinalized(matchId, winner, totalPot);
    }

    function claim(bytes32 matchId, address recipient) external {
        MatchData storage m = matchesById[matchId];
        require(m.finalized, "not-finalized");
        require(!m.settled, "already-settled");
        require(msg.sender == m.winner, "not-winner");
        require(recipient != address(0), "bad-recipient");

        m.settled = true;
        uint256 totalPot = 0;
        for (uint256 i = 0; i < m.players.length; i++) {
            totalPot += deposits[matchId][m.players[i]];
        }
        bool ok = IERC20Mini(m.token).transfer(recipient, totalPot);
        require(ok, "claim-transfer-failed");

        emit Claimed(matchId, msg.sender, recipient, totalPot);
    }

    function refundMatch(bytes32 matchId) external onlyOwner {
        MatchData storage m = matchesById[matchId];
        require(m.token != address(0), "match-missing");
        require(!m.finalized, "already-finalized");
        require(!m.settled, "already-settled");

        m.settled = true;
        uint256 totalPot = 0;
        for (uint256 i = 0; i < m.players.length; i++) {
            address player = m.players[i];
            uint256 amount = deposits[matchId][player];
            if (amount > 0) {
                totalPot += amount;
                bool ok = IERC20Mini(m.token).transfer(player, amount);
                require(ok, "refund-transfer-failed");
            }
        }

        emit Refunded(matchId, totalPot);
    }

    function getMatch(bytes32 matchId)
        external
        view
        returns (
            address token,
            uint256 fixedWager,
            uint64 depositDeadline,
            bool variableDeposit,
            address winner,
            bool finalized,
            bool settled,
            address[] memory players,
            uint256[] memory depositValues,
            uint256 totalPot
        )
    {
        MatchData storage m = matchesById[matchId];
        token = m.token;
        fixedWager = m.fixedWager;
        depositDeadline = m.depositDeadline;
        variableDeposit = m.variableDeposit;
        winner = m.winner;
        finalized = m.finalized;
        settled = m.settled;
        players = m.players;
        depositValues = new uint256[](players.length);
        for (uint256 i = 0; i < players.length; i++) {
            uint256 amount = deposits[matchId][players[i]];
            depositValues[i] = amount;
            totalPot += amount;
        }
    }
}
