""" RHEAS module containing the implementation of Kalman Filters

.. module:: kalman
   :synopsis: Implementation of Kalman filters

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import numpy as np
from scipy.linalg import sqrtm


class ENKF:

    def __init__(self, A, HA, d, E):
        """Initialize Ensemble Kalman Filter object using a state matrix *A*,
        a predicted measurement matrix *HA*, an observation vector *d*,
        and an observation error covariance matrix *R*."""
        if HA is not None:
            self.HA = HA
        else:
            self.HA = A
        self.A = np.mat(A)
        self.HA = np.mat(self.HA)
        self.d = np.mat(d)
        self.E = np.mat(E)
        self.ndim, self.nens = A.shape
        self.nobs = len(d)
        self.R = np.mat(
            np.diag(np.diag((1.0 / (self.nens - 1)) * self.E * self.E.T)))

    def analysis(self, dists):
        """Perform the analysis step of the Ensemble Kalman Filter and return
        an updated state matrix."""
        Dp = self.d + np.mean(self.E, axis=1) - self.HA
        HAp = self.HA - np.mean(self.HA, axis=1)
        HAY = HAp + self.E
        U, S, V = np.linalg.svd(HAY)
        i = np.where(np.cumsum(S) / np.sum(S[:self.nens]) > 0.999)[0]
        U = np.mat(U[:, :self.nens])
        L1 = np.mat(np.diag(1.0 / (S * S)))
        L1[i] = 0.0
        X1 = L1 * U.T
        X2 = X1 * Dp
        X3 = U * X2
        X4 = HAp.T * X3
        Ap = self.A - np.mean(self.A, axis=1)
        self.Aa = self.A + Ap * X4


class LETKF(ENKF):

    def analysis(self, dists):
        """Implements the Local Ensemble Transform Kalman Filter."""
        n = 1
        rho = 1.05
        localizer = lambda s: s
        Y = self.HA - np.mean(self.HA, axis=1)
        X = self.A - np.mean(self.A, axis=1)
        xa = np.zeros(self.A.shape)
        segs = [np.arange((i * n), min((i + 1) * n - 1, self.ndim - 1) + 1)
                for i in range(np.divide(self.ndim, n) + 1)]
        segs = [s for s in segs if len(s) > 0]
        for l in range(len(segs)):
            lx = segs[l]
            ly = range(self.nobs)  # localizer(segs[l])
            Xl = X[lx, :]
            Yl = Y[ly, :]
            Rl = np.diag(np.diag(np.array(self.R))[ly])
            C = Yl.T * np.linalg.pinv(Rl)
            P = np.linalg.pinv((self.nens - 1) *
                               np.eye(self.nens) / rho + C * Yl)
            W = np.real(sqrtm((self.nens - 1) * P))
            w = P * C * (np.mat(self.d[ly]) - np.mean(self.HA[ly, :], axis=1))
            W = W + w
            Xla = Xl * W + np.mean(self.A[lx, :], axis=1)
            xa[lx, :] = Xla
        self.Aa = xa


class SQRTENKF(ENKF):

    def analysis(self, dists):
        """Perform the analysis step of the Ensemble Kalman Filter and return
        an updated state matrix, using the square root algorithm from Evensen (2004)."""
        S = self.HA - np.mean(self.HA, axis=1)
        U0, S0, V0 = np.linalg.svd(S)
        S0 = 1.0 / S0
        i = np.where(np.cumsum(S0) / np.sum(S0) > 0.999)[0][0]
        S0[i:] = 0.0
        S0 = np.mat(np.diag(S0))
        U0 = np.mat(U0)
        X0 = S0 * U0.T * self.E
        U1, S1, V1 = np.linalg.svd(X0)
        U1 = np.mat(U1)
        X1 = U0 * S0.T * U1
        y0 = X1.T * (self.d - np.mean(self.HA, axis=1))
        S1i = np.mat(np.diag(1.0 / (1.0 + S1 ** 2.0)))
        y2 = S1i * y0
        y3 = X1 * y2
        y4 = S.T * y3
        Ap = self.A - np.mean(self.A, axis=1)
        xa = np.mean(self.A, axis=1) + Ap * y4
        S1q = np.mat(np.diag(np.sqrt(1.0 / (1.0 + S1 * S1))))
        X2 = S1q * X1.T * S
        U2, s2, V2 = np.linalg.svd(X2)
        _, _, Theta = np.linalg.svd(
            np.random.normal(0.0, 1.0, (self.nens, self.nens)))
        S2 = np.mat(np.zeros((self.nobs, self.nens)))
        S2[:self.nobs, :self.nobs] = np.mat(np.diag(s2))
        S2[S2 < 0.0] = 0.0
        I = np.mat(np.eye(self.nens))
        Stheta = np.sqrt(I - S2.T * S2) * Theta
        self.Aa = xa + Ap * V2.T * Stheta
