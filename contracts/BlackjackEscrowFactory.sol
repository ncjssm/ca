// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface IERC20 {
    function transfer(address to, uint256 value) external returns (bool);
    function transferFrom(address from, address to, uint256 value) external returns (bool);
}

contract BlackjackEscrowFactory {
    struct MatchData {
        address token;
        uint256 wager;
        uint64 depositDeadline;
        address winner;
        bool finalized;
        bool settled;
        address[] players;
    }

    address public owner;
    mapping(bytes32 => MatchData) private matchesById;
    mapping(bytes32 => mapping(address => uint256)) public deposits;
    mapping(bytes32 => mapping(address => bool)) public isPlayer;

    event MatchCreated(bytes32 indexed matchId, address indexed token, uint256 wager);
    event Deposited(bytes32 indexed matchId, address indexed player, uint256 amount);
    event MatchFinalized(bytes32 indexed matchId, address indexed winner);
    event Claimed(bytes32 indexed matchId, address indexed winner, address indexed recipient, uint256 amount);

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
        uint256 wager,
        address[] calldata players,
        uint64 depositDeadline
    ) external onlyOwner {
        require(matchesById[matchId].token == address(0), "match-exists");
        require(token != address(0), "bad-token");
        require(wager > 0, "bad-wager");
        require(players.length >= 2, "need-players");
        require(depositDeadline > block.timestamp, "bad-deadline");

        MatchData storage m = matchesById[matchId];
        m.token = token;
        m.wager = wager;
        m.depositDeadline = depositDeadline;
        for (uint256 i = 0; i < players.length; i++) {
            require(players[i] != address(0), "bad-player");
            require(!isPlayer[matchId][players[i]], "dup-player");
            isPlayer[matchId][players[i]] = true;
            m.players.push(players[i]);
        }

        emit MatchCreated(matchId, token, wager);
    }

    function deposit(bytes32 matchId) external {
        MatchData storage m = matchesById[matchId];
        require(m.token != address(0), "match-missing");
        require(!m.finalized, "finalized");
        require(block.timestamp <= m.depositDeadline, "deposit-closed");
        require(isPlayer[matchId][msg.sender], "not-player");
        require(deposits[matchId][msg.sender] == 0, "already-deposited");

        deposits[matchId][msg.sender] = m.wager;
        bool ok = IERC20(m.token).transferFrom(msg.sender, address(this), m.wager);
        require(ok, "transfer-failed");

        emit Deposited(matchId, msg.sender, m.wager);
    }

    function finalizeMatch(bytes32 matchId, address winner) external onlyOwner {
        MatchData storage m = matchesById[matchId];
        require(m.token != address(0), "match-missing");
        require(!m.finalized, "already-finalized");
        require(isPlayer[matchId][winner], "winner-not-player");

        for (uint256 i = 0; i < m.players.length; i++) {
            require(deposits[matchId][m.players[i]] == m.wager, "missing-deposit");
        }

        m.finalized = true;
        m.winner = winner;
        emit MatchFinalized(matchId, winner);
    }

    function claim(bytes32 matchId, address recipient) external {
        MatchData storage m = matchesById[matchId];
        require(m.finalized, "not-finalized");
        require(!m.settled, "already-settled");
        require(msg.sender == m.winner, "not-winner");
        require(recipient != address(0), "bad-recipient");

        m.settled = true;
        uint256 total = m.wager * m.players.length;
        bool ok = IERC20(m.token).transfer(recipient, total);
        require(ok, "claim-transfer-failed");

        emit Claimed(matchId, msg.sender, recipient, total);
    }

    function getMatch(bytes32 matchId)
        external
        view
        returns (
            address token,
            uint256 wager,
            uint64 depositDeadline,
            address winner,
            bool finalized,
            bool settled,
            address[] memory players,
            uint256[] memory depositValues
        )
    {
        MatchData storage m = matchesById[matchId];
        token = m.token;
        wager = m.wager;
        depositDeadline = m.depositDeadline;
        winner = m.winner;
        finalized = m.finalized;
        settled = m.settled;
        players = m.players;
        depositValues = new uint256[](players.length);
        for (uint256 i = 0; i < players.length; i++) {
            depositValues[i] = deposits[matchId][players[i]];
        }
    }
}
