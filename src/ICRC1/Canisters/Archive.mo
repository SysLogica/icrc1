import Prim "mo:prim";

import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Hash "mo:base/Hash";
import Result "mo:base/Result";

import ExperimentalCycles "mo:base/ExperimentalCycles";
import ExperimentalStableMemory "mo:base/ExperimentalStableMemory";
import Buffer "mo:base/Buffer";
import Int "mo:base/Int";
import Principal "mo:base/Principal";

import Itertools "mo:itertools/Iter";
import StableTrieMap "mo:StableTrieMap";
import U "../Utils";
import T "../Types";

shared ({ caller = ledger_canister_id }) actor class Archive() : async T.ArchiveInterface {

    type Transaction = T.Transaction;
    type MemoryBlock = {
        offset : Nat64;
        size : Nat;
    };

    stable let KiB = 1024;
    stable let GiB = KiB ** 3;
    stable let MEMORY_PER_PAGE : Nat64 = Nat64.fromNat(64 * KiB);
    stable let MIN_PAGES : Nat64 = 32; // 2MiB == 32 * 64KiB
    stable var PAGES_TO_GROW : Nat64 = 2048; // 64MiB
    stable let MAX_MEMORY = 32 * GiB;

    stable let BUCKET_SIZE = 1000;
    stable let MAX_TRANSACTIONS_PER_REQUEST = 5000;

    stable let MAX_TXS_LENGTH = 1000;

    stable var memory_pages : Nat64 = ExperimentalStableMemory.size();
    stable var total_memory_used : Nat64 = 0;

    stable var filled_buckets = 0;
    stable var trailing_txs = 0;

    stable let txStore = StableTrieMap.new<Nat, [MemoryBlock]>();
    stable var backupTxStore = StableTrieMap.new<Nat, [MemoryBlock]>();

    stable var prevArchive : T.ArchiveInterface = actor ("aaaaa-aa");
    stable var nextArchive : T.ArchiveInterface = actor ("aaaaa-aa");
    stable var first_tx : Nat = 0;
    stable var last_tx : Nat = 0;

    public shared query func get_prev_archive() : async T.ArchiveInterface {
        prevArchive;
    };

    public shared query func get_next_archive() : async T.ArchiveInterface {
        nextArchive;
    };

    public shared query func get_first_tx() : async Nat {
        first_tx;
    };

    public shared query func get_last_tx() : async Nat {
        last_tx;
    };

    public shared ({ caller }) func set_prev_archive(prev_archive : T.ArchiveInterface) : async Result.Result<(), Text> {

        if (caller != ledger_canister_id) {
            return #err("Unauthorized Access: Only the ledger canister can access this archive canister");
        };

        prevArchive := prev_archive;

        #ok();
    };

    public shared ({ caller }) func set_next_archive(next_archive : T.ArchiveInterface) : async Result.Result<(), Text> {

        if (caller != ledger_canister_id) {
            return #err("Unauthorized Access: Only the ledger canister can access this archive canister");
        };

        nextArchive := next_archive;

        #ok();
    };

    public shared ({ caller }) func set_first_tx(tx : Nat) : async Result.Result<(), Text> {

        if (caller != ledger_canister_id) {
            return #err("Unauthorized Access: Only the ledger canister can access this archive canister");
        };

        first_tx := tx;

        #ok();
    };

    public shared ({ caller }) func set_last_tx(tx : Nat) : async Result.Result<(), Text> {

        if (caller != ledger_canister_id) {
            return #err("Unauthorized Access: Only the ledger canister can access this archive canister");
        };

        last_tx := tx;

        #ok();
    };

    public shared func add_backup() : async () {
        backupTxStore := StableTrieMap.clone(
            txStore,
            Nat.equal,
            U.hash,
        );
    };

    public shared query func get_backup_transaction(tx_index : T.TxIndex) : async ?Transaction {
        let tx_max = Nat.max(tx_index, first_tx);
        let tx_off : Nat = tx_max - first_tx;
        let bucket_key = tx_off / BUCKET_SIZE;

        let opt_bucket = StableTrieMap.get(
            backupTxStore,
            Nat.equal,
            U.hash,
            bucket_key,
        );

        switch (opt_bucket) {
            case (?bucket) {
                let i = tx_off % BUCKET_SIZE;
                if (i < bucket.size()) {
                    ?get_tx(bucket[tx_off % BUCKET_SIZE]);
                } else {
                    null;
                };
            };
            case (_) {
                null;
            };
        };
    };

    public shared ({ caller }) func append_transactions(txs : [Transaction]) : async Result.Result<(), Text> {
        if (caller != ledger_canister_id) {
            // return #err("Unauthorized Access: Only the ledger canister can access this archive canister");
        };

        var txs_iter = txs.vals();

        if (trailing_txs > 0) {
            let last_bucket = StableTrieMap.get(
                txStore,
                Nat.equal,
                U.hash,
                filled_buckets,
            );

            switch (last_bucket) {
                case (?last_bucket) {
                    let new_bucket = Iter.toArray(
                        Itertools.take(
                            Itertools.chain(
                                last_bucket.vals(),
                                Iter.map(txs.vals(), store_tx),
                            ),
                            BUCKET_SIZE,
                        )
                    );

                    if (new_bucket.size() == BUCKET_SIZE) {
                        let offset = (BUCKET_SIZE - last_bucket.size()) : Nat;

                        txs_iter := Itertools.fromArraySlice(txs, offset, txs.size());
                    } else {
                        txs_iter := Itertools.empty();
                    };

                    store_bucket(new_bucket);
                };
                case (_) {};
            };
        };

        for (chunk in Itertools.chunks(txs_iter, BUCKET_SIZE)) {
            store_bucket(Array.map(chunk, store_tx));
        };

        #ok();
    };

    func total_txs() : Nat {
        (filled_buckets * BUCKET_SIZE) + trailing_txs;
    };

    public shared query func total_transactions() : async Nat {
        total_txs();
    };

    public shared query func get_transaction(tx_index : T.TxIndex) : async ?Transaction {
        let tx_max = Nat.max(tx_index, first_tx);
        let tx_off : Nat = tx_max - first_tx;
        let bucket_key = tx_off / BUCKET_SIZE;

        let opt_bucket = StableTrieMap.get(
            txStore,
            Nat.equal,
            U.hash,
            bucket_key,
        );

        switch (opt_bucket) {
            case (?bucket) {
                let i = tx_off % BUCKET_SIZE;
                if (i < bucket.size()) {
                    ?get_tx(bucket[tx_off % BUCKET_SIZE]);
                } else {
                    null;
                };
            };
            case (_) {
                null;
            };
        };
    };

    public shared query func get_transactions(req : T.GetTransactionsRequest) : async T.TransactionRange {
        let { start; length } = req;

        let length_max = Nat.max(0, length);
        let length_min = Nat.min(MAX_TXS_LENGTH, length_max);

        let start_max = Nat.max(start, first_tx);
        let start_off : Nat = start_max - first_tx;
        let end = start_off + length_min;
        let start_bucket = start_off / BUCKET_SIZE;
        let end_bucket = (Nat.min(end, total_txs()) / BUCKET_SIZE) + 1;

        get_transactions_from_buckets(start_off, end, start_bucket, end_bucket);
    };

    public shared query func remaining_capacity() : async Nat {
        MAX_MEMORY - Nat64.toNat(total_memory_used);
    };

    public shared query func max_memory() : async Nat {
        MAX_MEMORY;
    };

    public shared query func total_used() : async Nat {
        Nat64.toNat(total_memory_used);
    };

    /// Deposit cycles into this archive canister.
    public shared func deposit_cycles() : async () {
        let amount = ExperimentalCycles.available();
        let accepted = ExperimentalCycles.accept(amount);
        assert (accepted == amount);
    };

    public shared func deduplicate_transactions(fromTransaction : Nat) : async () {
        // TODO validate bucket is within range
        Debug.print("Started");

        let bucket_start = (fromTransaction / BUCKET_SIZE);
        let bucket_end = (total_txs() / BUCKET_SIZE);
        var bucket_txs = Buffer.Buffer<Transaction>(0);
        var tx_index = -1;
        let good_txs = Buffer.Buffer<Transaction>(BUCKET_SIZE * 2);
        var bad_bucket_index : Nat = 0;

        // find first bad bucket
        label buckets_loop for (bucket_i in Iter.range(bucket_start, bucket_end)) {
            // loop to detect which bucket a duplicate starts
            bucket_txs := Buffer.fromArray<Transaction>(get_bucket_transactions(bucket_i).transactions);
            Debug.print("BUCKET ID " # debug_show (bucket_i));
            label txs_loop for (t in bucket_txs.vals()) {
                if (t.index <= tx_index) {
                    bad_bucket_index := bucket_i;
                    break buckets_loop;
                };

                tx_index := t.index;
            };
        };
        Debug.print("BAD BUCKET: " # debug_show (bad_bucket_index));

        // starting at Bad Bucket, load all transactions and deduplicate
        tx_index := (bad_bucket_index * BUCKET_SIZE) - 1;
        label buckets_loop for (bucket_i in Iter.range(bad_bucket_index, bucket_end)) {
            // loop to detect which bucket a duplicate starts
            bucket_txs := Buffer.fromArray<Transaction>(get_bucket_transactions(bucket_i).transactions);
            Debug.print("LOAD TXS OF BUCKET ID " # debug_show (bucket_i));
            label txs_loop for (t in bucket_txs.vals()) {
                // if duplicate, ignore
                if (t.index <= tx_index) continue txs_loop;
                // if non-sequencial, tx is missing or unsorted, abort!
                if (t.index - tx_index > 1) {
                    Debug.print("T.index " # debug_show (t.index));
                    Debug.print("Tx_index " # debug_show (tx_index));
                    Debug.trap("Transactions reached a non-sequencial order, edge case not implemented.");
                };

                good_txs.add(t);
                tx_index := t.index;
            };
        };
        Debug.print("GOOD TXS SIZE: " # debug_show (good_txs.size()));

        // delete all buckets, starting from defect
        label delete_buckets_loop for (bucket_i in Iter.revRange(bucket_end, bad_bucket_index)) {
            ignore StableTrieMap.remove(
                txStore,
                Nat.equal,
                U.hash,
                Int.abs(bucket_i),
            );
            filled_buckets -= 1;
        };
        Debug.print("Deletion, Filled Buckets: " # debug_show (filled_buckets));

        // append transactions with correct array
        let res = await append_transactions(Buffer.toArray(good_txs));
        switch (res) {
            case (#ok) {};
            case (#err(msg)) {
                Debug.print("Append Error: " # debug_show (msg));
            };
        };
        Debug.print("Appended, Filled Buckets: " # debug_show (filled_buckets));
        Debug.print("Trailing Txs: " # debug_show (trailing_txs));

        // Debug.print(debug_show ({ transactions }));
    };

    func get_bucket_transactions(bucket : Nat) : T.TransactionRange {
        let start = BUCKET_SIZE * bucket;
        let start_off : Nat = start - first_tx;
        let end = start_off + BUCKET_SIZE;
        let start_bucket = start_off / BUCKET_SIZE;
        let end_bucket = (Nat.min(end, total_txs()) / BUCKET_SIZE) + 1;

        get_transactions_from_buckets(start_off, end, start_bucket, end_bucket);
    };

    func get_transactions_from_buckets(start_off : Nat, end : Nat, start_bucket : Nat, end_bucket : Nat) : T.TransactionRange {
        var iter = Itertools.empty<MemoryBlock>();
        label _loop for (i in Itertools.range(start_bucket, end_bucket)) {
            let opt_bucket = StableTrieMap.get(
                txStore,
                Nat.equal,
                U.hash,
                i,
            );

            switch (opt_bucket) {
                case (?bucket) {
                    if (i == start_bucket) {
                        iter := Itertools.fromArraySlice(bucket, start_off % BUCKET_SIZE, Nat.min(bucket.size(), end));
                    } else if (i + 1 == end_bucket) {
                        let bucket_iter = Itertools.fromArraySlice(bucket, 0, end % BUCKET_SIZE);
                        iter := Itertools.chain(iter, bucket_iter);
                    } else {
                        iter := Itertools.chain(iter, bucket.vals());
                    };
                };
                case (_) { break _loop };
            };
        };

        let transactions = Iter.toArray(
            Iter.map(
                Itertools.take(iter, MAX_TRANSACTIONS_PER_REQUEST),
                get_tx,
            )
        );

        { transactions };
    };

    func to_blob(tx : Transaction) : Blob {
        to_candid (tx);
    };

    func from_blob(tx : Blob) : Transaction {
        switch (from_candid (tx) : ?Transaction) {
            case (?tx) tx;
            case (_) Debug.trap("Could not decode tx blob");
        };
    };

    func store_tx(tx : Transaction) : MemoryBlock {
        let blob = to_blob(tx);

        if ((memory_pages * MEMORY_PER_PAGE) - total_memory_used < (MIN_PAGES * MEMORY_PER_PAGE)) {
            ignore ExperimentalStableMemory.grow(PAGES_TO_GROW);
            memory_pages += PAGES_TO_GROW;
        };

        let offset = total_memory_used;

        ExperimentalStableMemory.storeBlob(
            offset,
            blob,
        );

        let mem_block = {
            offset;
            size = blob.size();
        };

        total_memory_used += Nat64.fromNat(blob.size());
        mem_block;
    };

    func get_tx({ offset; size } : MemoryBlock) : Transaction {
        let blob = ExperimentalStableMemory.loadBlob(offset, size);

        let tx = from_blob(blob);
    };

    func store_bucket(bucket : [MemoryBlock]) {

        StableTrieMap.put(
            txStore,
            Nat.equal,
            U.hash,
            filled_buckets,
            bucket,
        );

        if (bucket.size() == BUCKET_SIZE) {
            filled_buckets += 1;
            trailing_txs := 0;
        } else {
            trailing_txs := bucket.size();
        };
    };
};
