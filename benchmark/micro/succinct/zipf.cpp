#include <algorithm>
#include <cmath>
#include <random>

/** Zipf-like random distribution.
 *
 *	Taken from https://stackoverflow.com/a/44154095/6055379
 * "Rejection-inversion to generate variates from monotone discrete
 * distributions", Wolfgang Hörmann and Gerhard Derflinger
 * ACM TOMACS 6.3 (1996): 169-184
 */
template<class INT_TYPE = uint64_t, class REAL_TYPE = double>
class Zipf {
public:
    typedef REAL_TYPE input_type;
    typedef INT_TYPE result_type;

    static_assert(std::numeric_limits<INT_TYPE>::is_integer, "");
    static_assert(!std::numeric_limits<REAL_TYPE>::is_integer, "");

    Zipf(const INT_TYPE n=std::numeric_limits<INT_TYPE>::max(),
                      const REAL_TYPE q=1.0)
        : n(n)
        , q(q)
        , H_x1(H(1.5) - 1.0)
        , H_n(H(n + 0.5))
        , dist(H_x1, H_n)
    {}

    INT_TYPE operator()(std::mt19937& rng)
    {
        while (true) {
            const REAL_TYPE u = dist(rng);
            const REAL_TYPE x = H_inv(u);
            const INT_TYPE  k = clamp<INT_TYPE>(std::round(x), 1, n);
            if (u >= H(k + 0.5) - h(k)) {
                return k;
            }
        }
    }

private:
    /** Clamp x to [min, max]. */
    template<typename T>
    static constexpr T clamp(const T x, const T min, const T max)
    {
        return std::max(min, std::min(max, x));
    }

    /** exp(x) - 1 / x */
    static double
    expxm1bx(const double x)
    {
        return (std::abs(x) > epsilon)
            ? std::expm1(x) / x
            : (1.0 + x/2.0 * (1.0 + x/3.0 * (1.0 + x/4.0)));
    }

    /** H(x) = log(x) if q == 1, (x^(1-q) - 1)/(1 - q) otherwise.
     * H(x) is an integral of h(x).
     *
     * Note the numerator is one less than in the paper order to work with all
     * positive q.
     */
    const REAL_TYPE H(const REAL_TYPE x)
    {
        const REAL_TYPE log_x = std::log(x);
        return expxm1bx((1.0 - q) * log_x) * log_x;
    }

    /** log(1 + x) / x */
    static REAL_TYPE
    log1pxbx(const REAL_TYPE x)
    {
        return (std::abs(x) > epsilon)
            ? std::log1p(x) / x
            : 1.0 - x * ((1/2.0) - x * ((1/3.0) - x * (1/4.0)));
    }

    /** The inverse function of H(x) */
    const REAL_TYPE H_inv(const REAL_TYPE x)
    {
        const REAL_TYPE t = std::max(-1.0, x * (1.0 - q));
        return std::exp(log1pxbx(t) * x);
    }

    /** That hat function h(x) = 1 / (x ^ q) */
    const REAL_TYPE h(const REAL_TYPE x)
    {
        return std::exp(-q * std::log(x));
    }

    static constexpr REAL_TYPE epsilon = 1e-8;

    INT_TYPE                                  n;     ///< Number of elements
    REAL_TYPE                                 q;     ///< Exponent
    REAL_TYPE                                 H_x1;  ///< H(x_1)
    REAL_TYPE                                 H_n;   ///< H(n)
    std::uniform_real_distribution<REAL_TYPE> dist;  ///< [H(x_1), H(n)]
};
