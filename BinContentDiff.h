/*! \file BinContentDiff.h file declares the dqm_algorithms::BinContentDiff  class.
*/

#ifndef DQM_ALGORITHMS_BINCONTENTDIFF_H
#define DQM_ALGORITHMS_BINCONTENTDIFF_H

#include <dqm_core/Algorithm.h>

namespace dqm_algorithms
{
	struct BinContentDiff : public dqm_core::Algorithm
        {
	  BinContentDiff();

	  ~BinContentDiff();

	    //overwrites virtual functions
	  BinContentDiff * clone( );
	  dqm_core::Result * execute( const std::string & , const TObject & , const dqm_core::AlgorithmConfig & );
	  void  printDescription();
	};
}

#endif // DQM_ALGORITHMS_BINCONTENTDIFF_H
