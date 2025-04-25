//package com.swirlds.common.test.fixtures.platform;
//
//import com.swirlds.common.crypto.Cryptography;
//import com.swirlds.common.merkle.MerkleInternal;
//import com.swirlds.common.merkle.MerkleLeaf;
//import com.swirlds.common.merkle.MerkleNode;
//import com.swirlds.common.merkle.crypto.MerkleCryptography;
//import com.swirlds.common.test.fixtures.crypto.CryptoRandomUtils;
//import java.util.List;
//import java.util.Random;
//import java.util.concurrent.Future;
//import org.hiero.base.concurrent.futures.StandardFuture;
//import org.hiero.consensus.model.crypto.Hash;
//
//public class FakeMerkleCryptography implements MerkleCryptography {
//    @Override
//    public Hash digestSync(MerkleInternal node, boolean setHash) {
//        final Hash hash = CryptoRandomUtils.randomHash(new Random());
//        node.setHash(hash);
//        return hash;
//    }
//
//    @Override
//    public Hash digestSync(MerkleInternal node, List<Hash> childHashes, boolean setHash) {
//        final Hash hash = CryptoRandomUtils.randomHash(new Random());
//        node.setHash(hash);
//        return hash;
//    }
//
//    @Override
//    public Hash digestSync(MerkleLeaf leaf) {
//        final Hash hash = CryptoRandomUtils.randomHash(new Random());
//        leaf.setHash(hash);
//        return hash;
//    }
//
//    @Override
//    public Hash digestTreeSync(MerkleNode root) {
////        if (root == null) {
////            return Cryptography.NULL_HASH;
////        }
////
////        final Hash hash = CryptoRandomUtils.randomHash(new Random());
////        root.setHash(hash);
////        return hash;
//
//        return Cryptography.NULL_HASH;
//    }
//
//    @Override
//    public Future<Hash> digestTreeAsync(MerkleNode root) {
////        if (root == null) {
//            return new StandardFuture<>(Cryptography.NULL_HASH);
////        } else if (root.getHash() != null) {
////            return new StandardFuture<>(root.getHash());
////        } else {
////            FutureMerkleHash resultFuture = new FutureMerkleHash();
////            ResultTask resultTask = new ResultTask(root, resultFuture);
////            new TraverseTask(root, resultTask).send();
////            return resultFuture;
////        }
//    }
//}
